using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Exchanges.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

/// <summary>
/// Proves against a real broker that a publish and the delivery it causes land in one trace,
/// and that the delivery span carries how the message was settled.
/// </summary>
public sealed class TracingIntegrationTests(RabbitMqContainerFixture fixture)
    : IClassFixture<RabbitMqContainerFixture>
{
    [IntegrationFact]
    public async Task PublishAndDeliver_ShareOneTrace_AndTheDeliveryRecordsItsSettlement()
    {
        var finished = new List<Activity>();
        var subscribed = RabbitTracing.SourceNames;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => subscribed.Contains(source.Name),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { lock (finished) finished.Add(activity); }
        };
        ActivitySource.AddActivityListener(listener);

        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var url = await fixture.GetConnectionStringAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbit(o => o
                .UseConnectionUrl(url)
                .UseClientProvidedName("tracing-tests"),
            cs => cs.AddConnection("it-tracing", c =>
            {
                var exchange = c.AddDirectExchange("it-tracing-x").Durable();
                var queue = c.AddQueue<string>("it-tracing-q").Durable().BoundTo(exchange);

                c.AddConsumer(queue).Subscribe<string>(_ =>
                {
                    handled.TrySetResult();
                    return ValueTask.CompletedTask;
                });

                c.AddProducer<string>(exchange).RoutedTo("it-tracing-q").Persistent();
            }));

        await using var provider = services.BuildServiceProvider();

        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        await provider.GetRequiredService<IRabbitProducer>().PublishAsync("hello");

        var completed = await Task.WhenAny(handled.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(handled.Task, completed);

        // The listener is process-wide and test classes run in parallel, so the spans of this
        // test are picked out by their queue rather than by being the only ones recorded.
        Activity? deliver = null;

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (deliver is null)
        {
            Assert.True(DateTime.UtcNow < deadline, "The delivery span did not finish in time");

            lock (finished)
            {
                deliver = finished.FirstOrDefault(a =>
                    a.Kind == ActivityKind.Consumer && a.OperationName.Contains("it-tracing-q"));
            }

            if (deliver is null)
            {
                await Task.Delay(100);
            }
        }

        Activity? publish;
        lock (finished)
        {
            publish = finished.SingleOrDefault(a =>
                a.Kind == ActivityKind.Producer && a.SpanId == deliver.ParentSpanId);
        }

        Assert.NotNull(publish);
        // The trace context travels in the message headers, so the delivery continues the publish.
        Assert.Equal(publish.TraceId, deliver.TraceId);
        Assert.Equal(publish.SpanId, deliver.ParentSpanId);

        Assert.Equal(
            RabbitTracing.Settlement.Acked,
            deliver.TagObjects.SingleOrDefault(t => t.Key == RabbitTracing.SettlementTag).Value);
    }
}
