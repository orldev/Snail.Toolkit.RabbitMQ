using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>The connection and channel caches when an open fails or a caller gives up.</summary>
public sealed class ProviderChaosTests
{
    /// <summary>
    /// One caller giving up on a connection open does not cancel the others waiting on the same open: the broker here
    /// is a socket that accepts and never answers the handshake, so the second caller's own wait runs into the test's
    /// timeout, not into the first caller's cancellation. (Red before the 2026-10-21 fix: the first token was captured
    /// in the shared Lazy.)
    /// </summary>
    [Fact]
    public async Task ConnectionOpening_CancelledByOneCaller_DoesNotCancelAnother()
    {
        using var broker = new TcpListener(IPAddress.Loopback, 0);
        broker.Start();
        var holding = broker.AcceptTcpClientAsync();
        var options = new RabbitOptions();
        options.ConnectionFactory.Endpoint = new AmqpTcpEndpoint("127.0.0.1", ((IPEndPoint)broker.LocalEndpoint).Port);
        var provider = new RabbitConnectionProvider(NullLogger<RabbitChannel>.Instance, NullLogger<RabbitConnection>.Instance, Options.Create(options));
        var declaration = new RabbitConnectionDeclaration("chaos");
        using var impatient = new CancellationTokenSource();

        var first = provider.FromDeclarationAsync(declaration, impatient.Token);
        var second = provider.FromDeclarationAsync(declaration, CancellationToken.None);
        await Task.Delay(300);
        await impatient.CancelAsync();
        var firstOutcome = await Record.ExceptionAsync(() => first);
        var secondOutcome = await Record.ExceptionAsync(() => second.WaitAsync(TimeSpan.FromSeconds(3)));

        Assert.IsAssignableFrom<OperationCanceledException>(firstOutcome);
        Assert.IsType<TimeoutException>(secondOutcome);
        using var accepted = await holding;
    }

    /// <summary>
    /// A channel whose tx.select is refused is disposed, not left open on the connection until it closes.
    /// (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task ChannelOpening_WhenTxSelectFails_DisposesTheChannel()
    {
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.TxSelectAsync(It.IsAny<CancellationToken>())).ThrowsAsync(new InvalidOperationException("tx.select refused"));
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>())).ReturnsAsync(channel.Object);
        var declaration = new RabbitConnectionDeclaration("connection");
        var connections = new Mock<IRabbitConnectionProvider>();
        connections.Setup(c => c.FromDeclarationAsync(declaration, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RabbitConnection(NullLogger<RabbitChannel>.Instance, declaration, connection.Object));
        var provider = new RabbitChannelProvider(NullLogger<RabbitChannel>.Instance, connections.Object);
        var producer = new RabbitProducerDeclaration(typeof(string), declaration) { Transactional = true };

        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.FromDeclarationAsync(producer));

        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }
}
