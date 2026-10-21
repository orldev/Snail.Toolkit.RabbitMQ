using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;
using Testcontainers.RabbitMq;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>A broker of its own, with rabbitmqctl at hand to close connections and count channels.</summary>
public sealed class ChaosBrokerFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private RabbitMqContainer? _container;

    public async Task<RabbitMqContainer> StartedAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_container is null)
            {
                _container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
                await _container.StartAsync();
            }

            return _container;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<string> ControlAsync(params string[] command)
    {
        var container = await StartedAsync();
        var result = await container.ExecAsync(["rabbitmqctl", "-q", .. command]);

        return result.Stdout;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }
}

/// <summary>What only a live broker shows: how many deliveries a consumer count handles at once, what a reconnect leaves behind.</summary>
public sealed class BrokerChaosTests(ChaosBrokerFixture broker) : IClassFixture<ChaosBrokerFixture>
{
    private async Task<ServiceProvider> StartRabbitAsync(Action<IRabbitBuilder> connections)
    {
        var container = await broker.StartedAsync();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbit(o => o.UseConnectionUrl(container.GetConnectionString()).UseClientProvidedName("chaos"), connections);
        var provider = services.BuildServiceProvider();

        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        return provider;
    }

    private static async Task<bool> IsReachedAsync(Func<bool> condition, TimeSpan patience)
    {
        var deadline = DateTime.UtcNow + patience;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                return false;
            }

            await Task.Delay(50);
        }

        return true;
    }

    /// <summary>
    /// Two consumers of one declaration share one channel whose dispatcher has one worker per consumer: with two
    /// messages in the queue and a handler that waits, both handlers run at once. (Red before the 2026-10-21 fix: one.)
    /// </summary>
    [IntegrationFact]
    public async Task CountOfTwo_WithTwoMessagesWaiting_RunsTwoHandlersAtOnce()
    {
        var inFlight = 0;
        var atOnce = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<string, ValueTask> wait = async _ =>
        {
            var running = Interlocked.Increment(ref inFlight);
            Interlocked.Exchange(ref atOnce, Math.Max(atOnce, running));
            await release.Task;
            Interlocked.Decrement(ref inFlight);
        };
        await using var provider = await StartRabbitAsync(cs => cs.AddConnection("chaos-count", c =>
        {
            var queue = c.AddQueue<string>("chaos-count-q").Durable();
            c.AddConsumer(queue).Count(2).Prefetch(1).Subscribe(wait);
            c.AddProducer<string>().RoutedTo("chaos-count-q");
        }));
        var producer = provider.GetRequiredService<IRabbitProducer>();

        await producer.PublishAsync("one");
        await producer.PublishAsync("two");
        var reached = await IsReachedAsync(() => Volatile.Read(ref atOnce) == 2, TimeSpan.FromSeconds(3));
        release.SetResult();

        Assert.True(reached, $"at most {atOnce} handler(s) ran at once with Count(2)");
    }

    /// <summary>
    /// A producer channel evicted when the broker closed the connection is disposed, so the client's automatic recovery
    /// does not reopen it beside the replacement: the broker counts as many channels after a reconnect as before.
    /// (Red before the 2026-10-21 fix: one more idle channel per reconnect.)
    /// </summary>
    [IntegrationFact]
    public async Task ConnectionRecovery_LeavesNoOrphanProducerChannel()
    {
        await using var provider = await StartRabbitAsync(cs => cs.AddConnection("chaos-recovery", c =>
            c.AddProducer<string>().RoutedTo("chaos-nowhere")));
        var producer = provider.GetRequiredService<IRabbitProducer>();
        await producer.PublishAsync("before");
        var before = await broker.ControlAsync("list_channels", "connection", "number");

        foreach (var pid in (await broker.ControlAsync("list_connections", "pid")).Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            await broker.ControlAsync("close_connection", pid.Trim(), "chaos");
        }

        var recovered = await IsReachedAsync(() => producer.PublishAsync("after").AsTask().ContinueWith(t => t.IsCompletedSuccessfully, TaskScheduler.Default).Result, TimeSpan.FromSeconds(30));
        var after = await broker.ControlAsync("list_channels", "connection", "number");

        Assert.True(recovered, "the producer did not recover within 30 seconds");
        Assert.True(Rows(before) == Rows(after), $"channels before the reconnect:\n{before}channels after:\n{after}");
    }

    /// <summary>
    /// A publish right after the broker closed the connection waits for the client's recovery instead of failing, and
    /// the message arrives: one call, no retry loop in the caller.
    /// </summary>
    [IntegrationFact]
    public async Task PublishAfterAForcedClose_WaitsForTheRecovery_AndTheMessageArrives()
    {
        var warmedUp = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrivedAfter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<string, ValueTask> receive = payload =>
        {
            var arrival = payload == "after" ? arrivedAfter : warmedUp;
            arrival.TrySetResult();
            return ValueTask.CompletedTask;
        };
        await using var provider = await StartRabbitAsync(cs => cs
            .AddConnection("chaos-wait-sub", c => c.AddConsumer(c.AddQueue<string>("chaos-wait-q").Durable()).Subscribe(receive))
            .AddConnection("chaos-wait-pub", c => c.AddProducer<string>().RoutedTo("chaos-wait-q")));
        var producer = provider.GetRequiredService<IRabbitProducer>();
        await producer.PublishAsync("warm-up");
        await warmedUp.Task.WaitAsync(TimeSpan.FromSeconds(10));

        foreach (var pid in (await broker.ControlAsync("list_connections", "pid")).Split('\n', StringSplitOptions.RemoveEmptyEntries).Skip(1))
        {
            await broker.ControlAsync("close_connection", pid.Trim(), "chaos");
        }

        var publishing = Record.ExceptionAsync(() => producer.PublishAsync("after").AsTask());
        var publishFailure = await publishing.WaitAsync(TimeSpan.FromSeconds(40));
        var arrival = await Record.ExceptionAsync(() => arrivedAfter.Task.WaitAsync(TimeSpan.FromSeconds(40)));

        Assert.Null(publishFailure);
        Assert.Null(arrival);
    }

    /// <summary>The channels in a listing: its lines without the column header.</summary>
    private static int Rows(string listing) => listing.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length - 1;
}
