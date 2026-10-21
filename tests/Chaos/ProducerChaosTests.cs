using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>The producer under a serializer that lies and under many publishes at once.</summary>
public sealed class ProducerChaosTests
{
    private readonly Mock<IChannel> _channel = new();
    private readonly RabbitConnectionDeclaration _connection = new("connection");

    private RabbitProducer ProducerOver(RabbitOptions options)
    {
        var declaration = new RabbitProducerDeclaration(typeof(string), _connection) { RoutingKey = "queue" };
        options.ProducerDeclarations[typeof(string)] = declaration;
        var channel = new RabbitChannel(_connection, _channel.Object, NullLogger<RabbitChannel>.Instance) { TracksConfirmations = true };
        var channels = new Mock<IRabbitChannelProvider>();
        channels.Setup(c => c.FromDeclarationAsync(It.IsAny<RabbitProducerDeclaration>(), It.IsAny<CancellationToken>())).ReturnsAsync(channel);

        return new RabbitProducer(Options.Create(options), channels.Object, new Mock<IRabbitConnectionProvider>(MockBehavior.Strict).Object, NullLogger<RabbitProducer>.Instance);
    }

    /// <summary>
    /// A custom serializer that returns null fails the publish with an error that names the serializer, not with a
    /// NullReferenceException from inside the body. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public async Task PublishAsync_WithASerializerReturningNull_FailsWithAClearError()
    {
        var options = new RabbitOptions().UseSerialization(_ => null!, (_, _) => "payload");

        var exception = await Record.ExceptionAsync(() => ProducerOver(options).PublishAsync("payload").AsTask());

        Assert.NotNull(exception);
        Assert.IsNotType<NullReferenceException>(exception);
    }

    /// <summary>
    /// 64 publishes in flight at once over a channel that confirms later: every body reaches the channel once. The
    /// per-thread properties and the pooled bodies survive the overlap.
    /// </summary>
    [Fact]
    public async Task PublishAsync_SixtyFourAtOnce_AllComplete()
    {
        var bodies = new System.Collections.Concurrent.ConcurrentBag<string>();
        _channel.Setup(c => c.BasicPublishAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(async (_, _, _, _, body, _) =>
            {
                bodies.Add(System.Text.Encoding.UTF8.GetString(body.Span));
                await Task.Delay(Random.Shared.Next(1, 5));
            });
        var producer = ProducerOver(new RabbitOptions());

        await Task.WhenAll(Enumerable.Range(0, 64).Select(i => Task.Run(() => producer.PublishAsync($"message-{i}").AsTask())));

        Assert.Equal(64, bodies.Distinct(StringComparer.Ordinal).Count());
    }
}
