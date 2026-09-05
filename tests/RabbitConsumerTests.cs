using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class RabbitConsumerTests
{
    private readonly Mock<IChannel> _channel = new();
    private readonly RabbitQueueDeclaration _queue;
    private readonly RabbitConsumerDeclaration _declaration;

    public RabbitConsumerTests()
    {
        var connection = new RabbitConnectionDeclaration("connection");
        _queue = new RabbitQueueDeclaration(connection, "queue");
        _declaration = new RabbitConsumerDeclaration(typeof(string), connection, [_queue]);
    }

    private RabbitConsumer CreateConsumer()
    {
        return new RabbitConsumer(
            _channel.Object,
            new RabbitOptions(),
            new ServiceCollection().BuildServiceProvider(),
            _queue,
            _declaration,
            NullLogger.Instance);
    }

    private Task DeliverAsync(string body, bool redelivered = false, BasicProperties? properties = null)
    {
        return CreateConsumer().HandleBasicDeliverAsync(
            "tag", 1UL, redelivered, string.Empty, "queue",
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body));
    }

    private static BasicProperties PropertiesWithDeaths(string queue, long rejectedCount)
    {
        return new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-death"] = new List<object>
                {
                    new Dictionary<string, object>
                    {
                        ["queue"] = Encoding.UTF8.GetBytes(queue),
                        ["reason"] = Encoding.UTF8.GetBytes("rejected"),
                        ["count"] = rejectedCount
                    }
                }
            }
        };
    }

    [Fact]
    public async Task DeserializationFailure_RejectsWithoutRequeue_AndDoesNotThrow()
    {
        await DeliverAsync("not a json payload");

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NullPayload_RejectsWithoutRequeue_AndDoesNotThrow()
    {
        await DeliverAsync("null");

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Success_InvokesHandlerAndAcks()
    {
        string? handled = null;
        _declaration.Subscriptions.Add((_, payload, _) =>
        {
            handled = (string)payload;
            return ValueTask.CompletedTask;
        });

        await DeliverAsync("\"hello\"");

        Assert.Equal("hello", handled);
        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandlerFailure_WithoutRetry_RequeuesOnFirstDelivery()
    {
        _declaration.Requeue = true;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"");

        _channel.Verify(c => c.BasicNackAsync(1UL, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandlerFailure_WithoutRetry_DoesNotRequeueRedelivered()
    {
        _declaration.Requeue = true;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"", redelivered: true);

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_RejectsIntoRetryQueue()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"");

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_KeepsRetryingBelowBudget()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"", properties: PropertiesWithDeaths("queue", rejectedCount: 1));

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_ExhaustedAttempts_AcksAndDrops()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"", properties: PropertiesWithDeaths("queue", rejectedCount: 2));

        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Poison_WithRetry_AcksAndDrops_InsteadOfCyclingThroughRetryQueue()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;

        await DeliverAsync("not a json payload");

        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Poison_WithParkingQueue_ParksAndAcks()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _queue.ParkingQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.failed");

        await DeliverAsync("not a json payload");

        _channel.Verify(c => c.BasicPublishAsync(
            string.Empty, "queue.failed", false,
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_ExhaustedAttempts_WithParkingQueue_ParksAndAcks()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _queue.ParkingQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.failed");
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"", properties: PropertiesWithDeaths("queue", rejectedCount: 2));

        _channel.Verify(c => c.BasicPublishAsync(
            string.Empty, "queue.failed", false,
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ParkingPublishFailure_RejectsForAnotherRetryCycle_InsteadOfLosingTheMessage()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 3;
        _queue.ParkingQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.failed");
        _channel.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("broker is gone"));

        await DeliverAsync("not a json payload");

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandlerFailure_SettlesTheMessage_WithoutEscapingToTheDispatcher()
    {
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        // A settled message must not also surface as a dispatcher callback exception:
        // that is what closes the channel and takes the consumer down with it.
        var exception = await Record.ExceptionAsync(() => DeliverAsync("\"hello\""));

        Assert.Null(exception);
        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandlerFailure_WithAutoAck_SettlesNothing_AndDoesNotThrow()
    {
        _declaration.AutoAck = true;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        var exception = await Record.ExceptionAsync(() => DeliverAsync("\"hello\""));

        Assert.Null(exception);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Settlement_NeverUsesMultiple_SoOtherConsumersOnTheChannelAreUntouched()
    {
        _declaration.Requeue = true;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"");

        _channel.Verify(c => c.BasicNackAsync(It.IsAny<ulong>(), true, It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), true, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandlerFailure_WithRetry_IgnoresDeathsOfOtherQueues()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 2;
        _declaration.Subscriptions.Add((_, _, _) => throw new InvalidOperationException("boom"));

        await DeliverAsync("\"hello\"", properties: PropertiesWithDeaths("other-queue", rejectedCount: 5));

        _channel.Verify(c => c.BasicNackAsync(1UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
