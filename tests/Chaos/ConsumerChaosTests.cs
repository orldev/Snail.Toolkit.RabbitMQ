using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>
/// What a delivery does to a message when the world around the handler misbehaves: the host shuts down, the scope
/// holds an asynchronous resource, the channel dies under the acknowledgement.
/// </summary>
public sealed class ConsumerChaosTests
{
    private readonly Mock<IChannel> _channel = new();
    private readonly RabbitQueueDeclaration _queue;
    private readonly RabbitConsumerDeclaration _declaration;

    public ConsumerChaosTests()
    {
        var connection = new RabbitConnectionDeclaration("connection");
        _queue = new RabbitQueueDeclaration(connection, "queue");
        _declaration = new RabbitConsumerDeclaration(typeof(string), connection, [_queue]);
    }

    private Task DeliverAsync(IServiceProvider services, CancellationToken cancellationToken = default)
    {
        var consumer = new RabbitConsumer(_channel.Object, new RabbitOptions(), services, _queue, _declaration, NullLogger.Instance);

        return consumer.HandleBasicDeliverAsync("tag", 1UL, false, string.Empty, "queue", new BasicProperties(), Encoding.UTF8.GetBytes("\"hello\""), cancellationToken);
    }

    /// <summary>A scoped service that can only be disposed asynchronously, as a database connection or a client of a modern SDK is.</summary>
    public sealed class AsyncOnlyResource : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// The delivery scope is disposed asynchronously when it can be: a scoped service that is only IAsyncDisposable
    /// used to make the container throw after the ack, on every delivery. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task Delivery_WithAnAsyncOnlyScopedService_CompletesWithoutThrowing()
    {
        var services = new ServiceCollection().AddScoped<AsyncOnlyResource>().BuildServiceProvider();
        _declaration.AddSubscription((scope, _, _) =>
        {
            scope.ServiceProvider.GetRequiredService<AsyncOnlyResource>();
            return ValueTask.CompletedTask;
        });

        var exception = await Record.ExceptionAsync(() => DeliverAsync(services));

        _channel.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
        Assert.Null(exception);
    }

    /// <summary>
    /// A handler that honours the cancellation of a shutdown throws OperationCanceledException; that is the host
    /// stopping, not the message failing, so the message goes back to the queue whole instead of spending the one
    /// attempt and being dropped. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task CancelledDelivery_OnARetryingQueueWithOneAttempt_IsNotDropped()
    {
        _queue.RetryQueue = new RabbitQueueDeclaration(_queue.ConnectionDeclaration, "queue.retry");
        _queue.MaxAttempts = 1;
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();
        _declaration.AddSubscription((_, _, token) =>
        {
            token.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        });

        await DeliverAsync(new ServiceCollection().BuildServiceProvider(), shutdown.Token);

        _channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        _channel.Verify(c => c.BasicNackAsync(1UL, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// Without a retry queue and without Requeue, a cancelled delivery is still returned to the queue, never rejected:
    /// a shutdown must not dead-letter or discard the message the handler was told to stop working on.
    /// </summary>
    [Fact]
    public async Task CancelledDelivery_WithoutRetry_IsRequeuedForTheNextConsumer()
    {
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();
        _declaration.AddSubscription((_, _, token) =>
        {
            token.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        });

        await DeliverAsync(new ServiceCollection().BuildServiceProvider(), shutdown.Token);

        _channel.Verify(c => c.BasicNackAsync(1UL, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// A channel that died while the handler ran makes the acknowledgement fail; that is logged, not thrown to the
    /// dispatcher, since the broker redelivers an unacknowledged message once the channel is gone.
    /// </summary>
    [Fact]
    public async Task AckFailure_AfterASuccessfulHandler_DoesNotEscapeToTheDispatcher()
    {
        _declaration.AddSubscription((_, _, _) => ValueTask.CompletedTask);
        _channel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("channel closed"));

        var exception = await Record.ExceptionAsync(() => DeliverAsync(new ServiceCollection().BuildServiceProvider()));

        Assert.Null(exception);
    }
}
