using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Tests;

/// <summary>
/// Covers what this library adds to the spans <c>RabbitMQ.Client</c> emits: the settlement of a delivery.
/// </summary>
public sealed class TracingTests : IDisposable
{
    private const string SourceName = "Snail.Toolkit.RabbitMQ.Tests.Delivery";

    private readonly Mock<IChannel> _channel = new();
    private readonly RabbitQueueDeclaration _queue;
    private readonly RabbitConsumerDeclaration _declaration;
    private readonly ActivitySource _source = new(SourceName);
    private readonly ActivityListener _listener;

    public TracingTests()
    {
        var connection = new RabbitConnectionDeclaration("connection");
        _queue = new RabbitQueueDeclaration(connection, "queue");
        _declaration = new RabbitConsumerDeclaration(typeof(string), connection, [_queue]);

        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _source.Dispose();
    }

    /// <summary>
    /// Delivers a message inside an activity, the way the client's consumer dispatcher does.
    /// </summary>
    private async Task<Activity> DeliverAsync(string body, BasicProperties? properties = null)
    {
        using var activity = _source.StartActivity("deliver queue", ActivityKind.Consumer)!;

        var consumer = new RabbitConsumer(
            _channel.Object,
            new RabbitOptions(),
            new ServiceCollection().BuildServiceProvider(),
            _queue,
            _declaration,
            NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync(
            "tag", 1UL, false, string.Empty, "queue",
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body));

        return activity;
    }

    private static object? Tag(Activity activity, string tag) =>
        activity.TagObjects.SingleOrDefault(t => t.Key == tag).Value;

    [Fact]
    public async Task Success_RecordsAcked()
    {
        _declaration.AddSubscription((_, _, _) => ValueTask.CompletedTask);

        var activity = await DeliverAsync("\"hello\"");

        Assert.Equal(RabbitTracing.Settlement.Acked, Tag(activity, RabbitTracing.SettlementTag));
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
    }

    [Fact]
    public async Task HandlerFailure_RecordsRejection_AndMarksTheSpanFailed()
    {
        _declaration.AddSubscription((_, _, _) => throw new InvalidOperationException("boom"));

        var activity = await DeliverAsync("\"hello\"");

        Assert.Equal(RabbitTracing.Settlement.Rejected, Tag(activity, RabbitTracing.SettlementTag));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Contains(activity.Events, e => e.Name == "exception");
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_RecordsTheScheduledAttempt()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _declaration.AddSubscription((_, _, _) => throw new InvalidOperationException("boom"));

        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object>
                {
                    new Dictionary<string, object>
                    {
                        ["queue"] = Encoding.UTF8.GetBytes("queue"),
                        ["reason"] = Encoding.UTF8.GetBytes("rejected"),
                        ["count"] = 1L
                    }
                }
            }
        };

        var activity = await DeliverAsync("\"hello\"", properties);

        Assert.Equal(RabbitTracing.Settlement.RetryScheduled, Tag(activity, RabbitTracing.SettlementTag));
        Assert.Equal(2, Tag(activity, RabbitTracing.AttemptTag));
    }

    [Fact]
    public async Task PoisonMessage_IsMarkedPoison_AndRecordsRejection()
    {
        var activity = await DeliverAsync("not a json payload");

        Assert.Equal(true, Tag(activity, RabbitTracing.PoisonTag));
        Assert.Equal(RabbitTracing.Settlement.Rejected, Tag(activity, RabbitTracing.SettlementTag));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task PoisonMessage_WithParkingQueue_RecordsParking()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _queue.ParkingQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.failed");

        var activity = await DeliverAsync("not a json payload");

        Assert.Equal(RabbitTracing.Settlement.Parked, Tag(activity, RabbitTracing.SettlementTag));
    }

    [Fact]
    public async Task AutoAck_HandlerFailure_RecordsTheMessageAsLost()
    {
        _declaration.AutoAck = true;
        _declaration.AddSubscription((_, _, _) => throw new InvalidOperationException("boom"));

        var activity = await DeliverAsync("\"hello\"");

        Assert.Equal(RabbitTracing.Settlement.Lost, Tag(activity, RabbitTracing.SettlementTag));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public void SourceNames_CoverBothClientSources_AndCannotBeMutatedByACaller()
    {
        Assert.Equal(
            [RabbitMQActivitySource.PublisherSourceName, RabbitMQActivitySource.SubscriberSourceName],
            RabbitTracing.SourceNames);

        RabbitTracing.SourceNames[0] = "tampered";

        Assert.Equal(RabbitMQActivitySource.PublisherSourceName, RabbitTracing.SourceNames[0]);
    }

    /// <remarks>No ambient activity: the client starts none when nothing collects traces.</remarks>
    [Fact]
    public async Task WithoutAListener_TracingIsInert()
    {
        _declaration.AddSubscription((_, _, _) => ValueTask.CompletedTask);

        var consumer = new RabbitConsumer(
            _channel.Object,
            new RabbitOptions(),
            new ServiceCollection().BuildServiceProvider(),
            _queue,
            _declaration,
            NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync(
            "tag", 1UL, false, string.Empty, "queue",
            new BasicProperties(),
            Encoding.UTF8.GetBytes("\"hello\""));

        Assert.Null(Activity.Current);
        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
    }
}
