using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Queues;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>
/// How the hosted services start and stop against a client that answers: what a consumer count buys, what a
/// topology channel costs on the way out, what registering twice does.
/// </summary>
public sealed class LifetimeChaosTests
{
    private static RabbitOptions OptionsOf(Action<IRabbitBuilder> connections)
    {
        var services = new ServiceCollection();
        services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"), connections);

        return services.BuildServiceProvider().GetRequiredService<IOptions<RabbitOptions>>().Value;
    }

    private static Mock<IRabbitConnectionProvider> ConnectionsOver(Mock<IConnection> connection)
    {
        var connections = new Mock<IRabbitConnectionProvider>();
        connections.Setup(c => c.FromDeclarationAsync(It.IsAny<RabbitConnectionDeclaration>(), It.IsAny<CancellationToken>()))
            .Returns<RabbitConnectionDeclaration, CancellationToken>((declaration, _) =>
                Task.FromResult(new RabbitConnection(NullLogger<RabbitChannel>.Instance, declaration, connection.Object)));

        return connections;
    }

    /// <summary>
    /// Count(4) registers four consumers on one channel, so the channel opens with four dispatcher workers; with the
    /// client's default of one, the four consumers were handled one delivery at a time. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task ConsumerCount_OpensTheChannelWithMatchingDispatchConcurrency()
    {
        CreateChannelOptions? opened = null;
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.BasicConsumeAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object?>?>(), It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("tag");
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .Callback<CreateChannelOptions?, CancellationToken>((options, _) => opened = options)
            .ReturnsAsync(channel.Object);
        var options = OptionsOf(cs => cs.AddConnection("connection", c => c.AddConsumer(c.AddQueue<string>("queue")).Count(4)));
        var lifetime = new RabbitConsumerLifetime(new ServiceCollection().BuildServiceProvider(), Options.Create(options), ConnectionsOver(connection).Object, NullLogger<RabbitConsumerLifetime>.Instance);

        await lifetime.StartAsync(CancellationToken.None);

        Assert.Equal((ushort)4, opened?.ConsumerDispatchConcurrency);
    }

    /// <summary>
    /// The topology services dispose their channel asynchronously: the client's synchronous Dispose blocks on
    /// AbortAsync inside StartAsync. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task QueueTopology_DisposesItsChannelAsynchronously()
    {
        var channel = new Mock<IChannel>();
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>())).ReturnsAsync(channel.Object);
        var options = OptionsOf(cs => cs.AddConnection("connection", c => c.AddQueue<string>("queue").Durable()));
        var topology = new RabbitQueueTopology(Options.Create(options), ConnectionsOver(connection).Object);

        await topology.StartAsync(CancellationToken.None);

        channel.Verify(c => c.DisposeAsync(), Times.Once);
        channel.Verify(c => c.Dispose(), Times.Never);
    }

    /// <summary>
    /// Two AddRabbit calls (two features each adding their own connection) still start the topology and the consumers
    /// once: AddHostedService registers by implementation type.
    /// </summary>
    [Fact]
    public void AddRabbit_CalledTwice_StartsEachHostedServiceOnce()
    {
        var services = new ServiceCollection();
        Action<IRabbitConnectionBuilder> nothing = _ => { };

        services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"), cs => cs.AddConnection("orders", nothing));
        services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"), cs => cs.AddConnection("billing", nothing));

        Assert.Equal(3, services.Count(d => d.ServiceType == typeof(IHostedService)));
    }

    /// <summary>
    /// AddConsumer with no queue is refused at declaration time instead of registering a consumer that never consumes.
    /// (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public void AddConsumer_WithoutQueues_IsRefused()
    {
        var services = new ServiceCollection();
        Action<IRabbitBuilder> withoutQueues = cs => cs.AddConnection("connection", c => c.AddConsumer<string>());

        var exception = Record.Exception(() => services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"), withoutQueues));

        Assert.IsAssignableFrom<ArgumentException>(exception);
    }
}
