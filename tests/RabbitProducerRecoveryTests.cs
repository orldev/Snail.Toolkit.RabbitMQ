using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Tests;

/// <summary>
/// A publish that finds its connection lost waits for the client's automatic recovery, then publishes once more;
/// one that failed for any other reason fails at once.
/// </summary>
public sealed class RabbitProducerRecoveryTests
{
    private readonly RabbitConnectionDeclaration _connection = new("connection");
    private readonly Mock<IChannel> _channel = new();
    private readonly Mock<IConnection> _client = new();
    private readonly Mock<IRabbitChannelProvider> _channels = new();
    private bool _isOpen;
    private AsyncEventHandler<AsyncEventArgs>? _onRecovered;

    public RabbitProducerRecoveryTests()
    {
        _client.SetupGet(c => c.IsOpen).Returns(() => _isOpen);
        _client.SetupAdd(c => c.RecoverySucceededAsync += It.IsAny<AsyncEventHandler<AsyncEventArgs>>())
            .Callback<AsyncEventHandler<AsyncEventArgs>>(handler => _onRecovered = handler);
        _channel.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
    }

    private static AlreadyClosedException ClosedBy(ShutdownInitiator initiator, ushort replyCode) =>
        new(new ShutdownEventArgs(initiator, replyCode, "closed"));

    private RabbitProducer Producer(RabbitOptions options)
    {
        options.ProducerDeclarations[typeof(string)] = new RabbitProducerDeclaration(typeof(string), _connection) { RoutingKey = "queue" };
        var connections = new Mock<IRabbitConnectionProvider>(MockBehavior.Strict);
        connections.Setup(c => c.FromDeclarationAsync(_connection, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RabbitConnection(NullLogger<RabbitChannel>.Instance, _connection, _client.Object));

        return new RabbitProducer(Options.Create(options), _channels.Object, connections.Object, NullLogger<RabbitProducer>.Instance);
    }

    private void FailFirstOpenWith(AlreadyClosedException failure)
    {
        var channel = new RabbitChannel(_connection, _channel.Object, NullLogger<RabbitChannel>.Instance);
        _channels.SetupSequence(c => c.FromDeclarationAsync(It.IsAny<RabbitProducerDeclaration>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure)
            .ReturnsAsync(channel);
    }

    private void RecoverAfter(TimeSpan delay)
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(delay);
            _isOpen = true;
            await _onRecovered!(_client.Object, AsyncEventArgs.Empty);
        });
    }

    [Fact]
    public async Task PublishAsync_WhenTheBrokerClosedTheConnection_WaitsForTheRecoveryAndPublishesOnce()
    {
        FailFirstOpenWith(ClosedBy(ShutdownInitiator.Peer, Constants.ConnectionForced));
        RecoverAfter(TimeSpan.FromMilliseconds(100));

        await Producer(new RabbitOptions()).PublishAsync("payload");

        _channel.Verify(c => c.BasicPublishAsync(
            It.IsAny<string>(), "queue", It.IsAny<bool>(),
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WhenTheSocketDropped_WaitsForTheRecoveryAndPublishesOnce()
    {
        FailFirstOpenWith(ClosedBy(ShutdownInitiator.Library, Constants.InternalError));
        RecoverAfter(TimeSpan.FromMilliseconds(100));

        await Producer(new RabbitOptions()).PublishAsync("payload");

        _channels.Verify(c => c.FromDeclarationAsync(It.IsAny<RabbitProducerDeclaration>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task PublishAsync_WhenTheRecoveryTakesLongerThanTheWait_FailsWithTheOriginalError()
    {
        var lost = ClosedBy(ShutdownInitiator.Peer, Constants.ConnectionForced);
        FailFirstOpenWith(lost);
        var options = new RabbitOptions().UseRecoveryWait(TimeSpan.FromMilliseconds(100));

        var exception = await Record.ExceptionAsync(() => Producer(options).PublishAsync("payload").AsTask());

        Assert.Same(lost, exception);
        _client.VerifyRemove(c => c.RecoverySucceededAsync -= It.IsAny<AsyncEventHandler<AsyncEventArgs>>(), Times.Once);
    }

    /// <summary>A channel the broker closed for a protocol error would fail the same way again, so nothing waits.</summary>
    [Fact]
    public async Task PublishAsync_WhenTheBrokerClosedOnlyTheChannel_FailsAtOnce()
    {
        var notFound = ClosedBy(ShutdownInitiator.Peer, Constants.NotFound);
        FailFirstOpenWith(notFound);

        var exception = await Record.ExceptionAsync(() => Producer(new RabbitOptions()).PublishAsync("payload").AsTask());

        Assert.Same(notFound, exception);
        Assert.Null(_onRecovered);
    }

    [Fact]
    public async Task PublishAsync_WithARecoveryWaitOfZero_FailsAtOnce()
    {
        var lost = ClosedBy(ShutdownInitiator.Peer, Constants.ConnectionForced);
        FailFirstOpenWith(lost);
        var options = new RabbitOptions().UseRecoveryWait(TimeSpan.Zero);

        var exception = await Record.ExceptionAsync(() => Producer(options).PublishAsync("payload").AsTask());

        Assert.Same(lost, exception);
        Assert.Null(_onRecovered);
    }

    [Fact]
    public async Task PublishAsync_CancelledWhileWaitingForTheRecovery_IsCancelled()
    {
        FailFirstOpenWith(ClosedBy(ShutdownInitiator.Peer, Constants.ConnectionForced));
        using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var exception = await Record.ExceptionAsync(() => Producer(new RabbitOptions()).PublishAsync("payload", cancellationToken: caller.Token).AsTask());

        Assert.IsAssignableFrom<OperationCanceledException>(exception);
    }

    [Fact]
    public void UseRecoveryWait_Negative_IsRefused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RabbitOptions().UseRecoveryWait(TimeSpan.FromSeconds(-1)));
    }
}
