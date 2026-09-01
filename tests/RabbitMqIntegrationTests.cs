using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Exceptions;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;
using Testcontainers.RabbitMq;

namespace Snail.Toolkit.RabbitMQ.Tests;

/// <summary>
/// Marks a test that requires Docker; skipped only when no Docker daemon is reachable.
/// </summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> DockerAvailable = new(DetectDocker);

    public IntegrationFactAttribute()
    {
        if (!DockerAvailable.Value)
        {
            Skip = "Docker is not available; RabbitMQ integration tests were skipped";
        }
    }

    private static bool DetectDocker()
    {
        try
        {
            using var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "info",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            });

            if (process is null)
            {
                return false;
            }

            if (!process.WaitForExit(10_000))
            {
                process.Kill(entireProcessTree: true);
                return false;
            }

            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>
/// Starts a single RabbitMQ container lazily and shares it across the tests of a class.
/// </summary>
public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private RabbitMqContainer? _container;

    public async Task<string> GetConnectionStringAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_container is null)
            {
                _container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
                await _container.StartAsync();
            }

            return _container.GetConnectionString();
        }
        finally
        {
            _gate.Release();
        }
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

public sealed class RabbitMqIntegrationTests(RabbitMqContainerFixture fixture)
    : IClassFixture<RabbitMqContainerFixture>
{
    private async Task<ServiceProvider> StartRabbitAsync(Action<IRabbitBuilder> connections)
    {
        var url = await fixture.GetConnectionStringAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbit(o => o
                .UseConnectionUrl(url)
                .UseClientProvidedName("integration-tests"),
            connections);

        var provider = services.BuildServiceProvider();

        // Registration order: exchanges, queues, consumers
        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        return provider;
    }

    private static async Task<T> WaitAsync<T>(Task<T> task, int seconds = 30)
    {
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(seconds)));
        Assert.Same(task, completed);
        return await task;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int seconds = 30)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition was not reached in time");
            await Task.Delay(100);
        }
    }

    [IntegrationFact]
    public async Task PoisonMessage_IsRejected_AndConsumerKeepsWorking()
    {
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var provider = await StartRabbitAsync(cs => cs
            .AddConnection("it-poison", c =>
            {
                // RabbitMQ 4 rejects transient non-exclusive queues by default
                var queue = c.AddQueue<string>("it-poison-q").Durable();

                c.AddConsumer(queue)
                    .Prefetch(1)
                    .Subscribe((string payload) =>
                    {
                        received.TrySetResult(payload);
                        return ValueTask.CompletedTask;
                    });

                c.AddProducer<int>().RoutedTo("it-poison-q");
                c.AddProducer<string>().RoutedTo("it-poison-q");
            }));

        var producer = provider.GetRequiredService<IRabbitProducer>();

        // An int cannot be deserialized into the consumer's string payload: a poison message.
        // With Prefetch(1) an unacked poison message would stall the consumer forever.
        Assert.True(await producer.PublishAsync(12345));
        Assert.True(await producer.PublishAsync("hello"));

        Assert.Equal("hello", await WaitAsync(received.Task));
    }

    [IntegrationFact]
    public async Task Retry_RedeliversWithBackoffUntilSuccess()
    {
        var attempts = 0;
        var succeeded = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var provider = await StartRabbitAsync(cs => cs
            .AddConnection("it-retry", c =>
            {
                var queue = c.AddQueue<string>("it-retry-q")
                    .Durable()
                    .WithRetry(TimeSpan.FromMilliseconds(500), maxAttempts: 3);

                c.AddConsumer(queue)
                    .Subscribe((string _) =>
                    {
                        var attempt = Interlocked.Increment(ref attempts);
                        if (attempt < 3)
                        {
                            throw new InvalidOperationException("transient failure");
                        }

                        succeeded.TrySetResult(attempt);
                        return ValueTask.CompletedTask;
                    });

                c.AddProducer<string>().RoutedTo("it-retry-q");
            }));

        var producer = provider.GetRequiredService<IRabbitProducer>();
        Assert.True(await producer.PublishAsync("retry-me"));

        Assert.Equal(3, await WaitAsync(succeeded.Task));
    }

    [IntegrationFact]
    public async Task Retry_ExhaustedAttempts_DropsMessage_AndConsumerKeepsWorking()
    {
        var badAttempts = 0;
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var provider = await StartRabbitAsync(cs => cs
            .AddConnection("it-drop", c =>
            {
                var queue = c.AddQueue<string>("it-drop-q")
                    .Durable()
                    .WithRetry(TimeSpan.FromMilliseconds(200), maxAttempts: 2);

                c.AddConsumer(queue)
                    .Subscribe((string payload) =>
                    {
                        if (payload == "bad")
                        {
                            Interlocked.Increment(ref badAttempts);
                            throw new InvalidOperationException("permanent failure");
                        }

                        received.TrySetResult(payload);
                        return ValueTask.CompletedTask;
                    });

                c.AddProducer<string>().RoutedTo("it-drop-q");
            }));

        var producer = provider.GetRequiredService<IRabbitProducer>();
        Assert.True(await producer.PublishAsync("bad"));

        await WaitUntilAsync(() => Volatile.Read(ref badAttempts) == 2);

        // One more retry cycle would have happened by now if the message were still circulating
        await Task.Delay(700);
        Assert.Equal(2, Volatile.Read(ref badAttempts));

        Assert.True(await producer.PublishAsync("good"));
        Assert.Equal("good", await WaitAsync(received.Task));
    }

    [IntegrationFact]
    public async Task Publish_MandatoryUnroutable_ThrowsBecauseConfirmationsAreOn()
    {
        await using var provider = await StartRabbitAsync(cs => cs
            .AddConnection("it-confirms", c =>
            {
                c.AddProducer<double>().RoutedTo("it-no-such-queue").Mandatory();
            }));

        var producer = provider.GetRequiredService<IRabbitProducer>();

        await Assert.ThrowsAnyAsync<PublishException>(() => producer.PublishAsync(1.5).AsTask());
    }
}
