using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Exchanges;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>One message published through the producer: declaration lookup, serialisation, properties, the channel call.</summary>
/// <remarks>
/// The channel accepts at once, so what is timed is the library's own work per publish, the part that runs for every
/// message whatever the broker. The channel comes from the library's own provider, which opened it once in the setup:
/// the state of every publish after the first. Overrides take the copy-the-declaration path a per-message id takes.
/// </remarks>
[MemoryDiagnoser]
public class Publishing
{
    private readonly Order _order = Order.Sample();

    private IRabbitProducer _producer = null!;

    private FakeChannel _channel = null!;

    /// <summary>Whether the broker's confirm arrives after the publish returned, as it does over a network.</summary>
    [Params(false, true)]
    public bool ConfirmsLater { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var options = new RabbitOptions();
        var connection = new RabbitConnectionDeclaration("main");
        var exchange = new RabbitExchangeDeclaration(connection, "direct", "orders");
        var declaration = new RabbitProducerDeclaration(typeof(Order), connection, exchange) { RoutingKey = "orders.created" };
        declaration.AddProperty(properties => properties.Persistent = true);
        options.ProducerDeclarations[typeof(Order)] = declaration;

        _channel = new FakeChannel { ConfirmsLater = ConfirmsLater };

        var channels = new RabbitChannelProvider(NullLogger<RabbitChannel>.Instance, new InProcessConnections(_channel));
        await channels.FromDeclarationAsync(declaration);

        _producer = new RabbitProducer(Options.Create(options), channels, new InProcessConnections(_channel), NullLogger<RabbitProducer>.Instance);

        await VerifyAsync(options, declaration);
    }

    [Benchmark(Baseline = true)]
    public ValueTask Publish()
    {
        var publishing = _producer.PublishAsync(_order);
        _channel.Acknowledge();

        return publishing;
    }

    [Benchmark]
    public ValueTask PublishWithOverrides()
    {
        var publishing = _producer.PublishAsync(_order, static producer => producer.MessageId("order-8c5f5d3e"));
        _channel.Acknowledge();

        return publishing;
    }

    /// <summary>
    /// Fails the run unless each publish sends exactly the bytes System.Text.Json writes for the order with its defaults,
    /// with exactly the properties the declaration and the overrides set, and the headers the client adds; a publish
    /// with overrides between two without checks that nothing of one publish reaches the next.
    /// </summary>
    private async Task VerifyAsync(RabbitOptions options, RabbitProducerDeclaration declaration)
    {
        var sent = new List<(byte[] Body, string Properties)>();
        var channel = new FakeChannel { Received = (body, properties) => sent.Add((body.ToArray(), Describe(properties))) };
        var channels = new RabbitChannelProvider(NullLogger<RabbitChannel>.Instance, new InProcessConnections(channel));
        await channels.FromDeclarationAsync(declaration);

        var producer = new RabbitProducer(Options.Create(options), channels, new InProcessConnections(channel), NullLogger<RabbitProducer>.Instance);
        await producer.PublishAsync(_order);
        await producer.PublishAsync(_order, static producer => producer.MessageId("order-8c5f5d3e"));
        await producer.PublishAsync(_order);

        var expected = JsonSerializer.SerializeToUtf8Bytes(_order);
        string[] properties =
        [
            Describe(new BasicProperties { Persistent = true, Headers = Sequence(1) }),
            Describe(new BasicProperties { Persistent = true, MessageId = "order-8c5f5d3e", Headers = Sequence(2) }),
            Describe(new BasicProperties { Persistent = true, Headers = Sequence(3) })
        ];

        if (sent.Count != properties.Length)
            throw new InvalidOperationException($"The producer published {sent.Count} messages instead of {properties.Length}.");

        for (var i = 0; i < properties.Length; i++)
        {
            if (!sent[i].Body.AsSpan().SequenceEqual(expected))
                throw new InvalidOperationException("The producer published other bytes than the default serializer writes.");

            if (sent[i].Properties != properties[i])
                throw new InvalidOperationException($"Publish {i + 1} carried {sent[i].Properties} instead of {properties[i]}.");
        }
    }

    private static Dictionary<string, object?> Sequence(long number) => new() { [Constants.PublishSequenceNumberHeader] = number };

    /// <summary>Every property the client writes, and the headers in the order it writes them.</summary>
    private static string Describe(IReadOnlyBasicProperties properties)
    {
        var headers = properties.Headers is null
            ? "none"
            : string.Join(", ", properties.Headers.Select(header => $"{header.Key}={header.Value}"));

        return $"ContentType={properties.ContentType}; ContentEncoding={properties.ContentEncoding}; Headers=[{headers}]; " +
               $"DeliveryMode={properties.DeliveryMode}; Priority={properties.Priority}; CorrelationId={properties.CorrelationId}; " +
               $"ReplyTo={properties.ReplyTo}; Expiration={properties.Expiration}; MessageId={properties.MessageId}; " +
               $"Timestamp={properties.Timestamp.UnixTime}; Type={properties.Type}; UserId={properties.UserId}; " +
               $"AppId={properties.AppId}; ClusterId={properties.ClusterId}";
    }
}
