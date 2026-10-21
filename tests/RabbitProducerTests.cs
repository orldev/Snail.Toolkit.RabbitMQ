using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Exchanges;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class RabbitProducerTests
{
    private readonly Mock<IChannel> _channel = new();
    private readonly RabbitConnectionDeclaration _connection = new("connection");
    private readonly List<string> _properties = [];
    private readonly List<IDictionary<string, object?>?> _headers = [];
    private byte[]? _sent;
    private long _sequence;
    private bool _addsSequenceNumbers;

    public RabbitProducerTests()
    {
        _channel.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>((_, _, _, properties, body, _) =>
            {
                _sent = body.ToArray();
                _properties.Add(Describe(properties));
                _headers.Add(properties.Headers);

                if (_addsSequenceNumbers)
                {
                    properties.Headers ??= new Dictionary<string, object?>();
                    properties.Headers[Constants.PublishSequenceNumberHeader] = ++_sequence;
                }
            })
            .Returns(ValueTask.CompletedTask);
    }

    public class Shipment
    {
        public string Carrier { get; set; } = "";
    }

    public sealed class TrackedShipment : Shipment
    {
        public string Tracking { get; set; } = "";
    }

    /// <summary>The default body is written into a pooled buffer; it must still be the bytes System.Text.Json writes, for the payload's runtime type.</summary>
    [Fact]
    public async Task PublishAsync_WithTheDefaultSerializer_SendsWhatSystemTextJsonWritesForTheRuntimeType()
    {
        Shipment payload = new TrackedShipment { Carrier = "DHL", Tracking = "JD014600006262" };

        await Producer<Shipment>(new RabbitOptions()).PublishAsync(payload);

        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(payload, payload.GetType()), _sent);
    }

    /// <summary>A body larger than the first rented buffer grows into a bigger one without losing what was written.</summary>
    [Fact]
    public async Task PublishAsync_WithABodyLargerThanTheFirstBuffer_SendsItWhole()
    {
        var payload = new Shipment { Carrier = new string('x', 5000) };

        await Producer<Shipment>(new RabbitOptions()).PublishAsync(payload);

        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(payload), _sent);
    }

    [Fact]
    public async Task PublishAsync_WithACustomSerializer_SendsItsBytes()
    {
        var options = new RabbitOptions().UseSerialization(payload => Encoding.UTF8.GetBytes(((Shipment)payload).Carrier), (_, _) => new Shipment());

        await Producer<Shipment>(options).PublishAsync(new Shipment { Carrier = "UPS" });

        Assert.Equal("UPS"u8.ToArray(), _sent);
    }

    /// <summary>
    /// The properties are used again from one publish to the next: each publish carries what its declaration and
    /// overrides set and nothing of the publish before.
    /// </summary>
    [Fact]
    public async Task PublishAsync_OneAfterAnother_CarriesOnlyItsOwnProperties()
    {
        var producer = Producer<Shipment>(new RabbitOptions(), declaration => declaration.AddProperty(p => p.Persistent = true));

        await producer.PublishAsync(new Shipment());
        await producer.PublishAsync(new Shipment(), p => p.MessageId("m-1").CorrelationId("c-1").Priority(5).Header("tenant", "a"));
        await producer.PublishAsync(new Shipment(), p => p.Header("tenant", "b"));
        await producer.PublishAsync(new Shipment());

        Assert.Equal(
        [
            "DeliveryMode=Persistent; Priority=0; MessageId=; CorrelationId=; Headers=none",
            "DeliveryMode=Persistent; Priority=5; MessageId=m-1; CorrelationId=c-1; Headers=tenant=a",
            "DeliveryMode=Persistent; Priority=0; MessageId=; CorrelationId=; Headers=tenant=b",
            "DeliveryMode=Persistent; Priority=0; MessageId=; CorrelationId=; Headers=none"
        ], _properties);
    }

    /// <summary>
    /// On a channel with publisher confirmations the client adds the sequence number to the headers, giving properties
    /// without headers a new dictionary; the producer hands it an empty one instead, emptied again for every publish.
    /// </summary>
    [Fact]
    public async Task PublishAsync_OnAChannelWithConfirmations_HandsTheClientEmptyHeaders()
    {
        _addsSequenceNumbers = true;
        var producer = Producer<Shipment>(new RabbitOptions(), tracksConfirmations: true);

        await producer.PublishAsync(new Shipment());
        await producer.PublishAsync(new Shipment());

        Assert.Equal(["Headers=empty", "Headers=empty"], _properties.Select(p => p[(p.IndexOf("Headers=", StringComparison.Ordinal))..]));
    }

    /// <summary>A transactional channel adds nothing to the headers, so properties without headers keep none.</summary>
    [Fact]
    public async Task PublishAsync_OnATransactionalChannel_SendsNoHeaders()
    {
        _channel.Setup(c => c.TxCommitAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var producer = Producer<Shipment>(new RabbitOptions(), declaration => declaration.Transactional = true);

        await producer.PublishAsync(new Shipment());

        Assert.Null(Assert.Single(_headers));
    }

    /// <summary>Headers an action sets are the action's: the producer neither replaces nor empties them.</summary>
    [Fact]
    public async Task PublishAsync_WithHeadersOfAnAction_LeavesThemAlone()
    {
        var shared = new Dictionary<string, object?> { ["source"] = "billing" };
        var producer = Producer<Shipment>(new RabbitOptions(), declaration => declaration.AddProperty(p => p.Headers = shared), tracksConfirmations: true);

        await producer.PublishAsync(new Shipment());

        Assert.Same(shared, Assert.Single(_headers));
        Assert.Equal("billing", shared["source"]);
    }

    /// <summary>
    /// Publishes overlap on one thread: each starts while the ones before still wait for the client, which reads the
    /// properties only after an await of its own and confirms late and out of order. Every publish must be handed
    /// properties no other publish in flight holds, and the client must read exactly the message's own properties.
    /// </summary>
    [Fact]
    public async Task PublishAsync_OverlappingOnOneThread_EachPublishReadsItsOwnProperties()
    {
        const int count = 8;
        var gates = new List<TaskCompletionSource>();
        var confirms = new List<TaskCompletionSource>();
        var handed = new List<BasicProperties>();
        var threads = new HashSet<int>();
        var read = new Dictionary<string, string>();
        long sequence = 0;

        async ValueTask PublishLater(BasicProperties properties, ReadOnlyMemory<byte> body)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var confirm = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var carrier = JsonSerializer.Deserialize<Shipment>(body.Span)!.Carrier;
            gates.Add(gate);
            confirms.Add(confirm);
            handed.Add(properties);
            threads.Add(Environment.CurrentManagedThreadId);

            await gate.Task;

            properties.Headers ??= new Dictionary<string, object?>();
            properties.Headers[Constants.PublishSequenceNumberHeader] = ++sequence;
            read[carrier] = DescribeWithoutSequence(properties);

            await confirm.Task;
        }

        _channel.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>((_, _, _, properties, body, _) => PublishLater(properties, body));

        var producer = Producer<Shipment>(new RabbitOptions(), declaration => declaration.AddProperty(p => p.Persistent = true), tracksConfirmations: true);

        await OneThread.RunAsync(async () =>
        {
            for (var round = 0; round < 2; round++)
            {
                gates.Clear();
                confirms.Clear();
                handed.Clear();

                var publishes = Enumerable.Range(0, count)
                    .Select(i => producer.PublishAsync(new Shipment { Carrier = $"r{round}-{i}" }, Overrides(i)).AsTask())
                    .ToList();

                Assert.Equal(count, handed.Distinct(ReferenceEqualityComparer.Instance).Count());

                for (var i = count - 1; i >= 0; i--)
                {
                    gates[i].SetResult();
                    await Task.Yield();
                }

                foreach (var i in new[] { 3, 0, 7, 5, 1, 6, 2, 4 })
                {
                    confirms[i].SetResult();
                    await Task.Yield();
                }

                await Task.WhenAll(publishes);
            }
        });

        Assert.Single(threads);

        for (var round = 0; round < 2; round++)
        {
            for (var i = 0; i < count; i++)
            {
                var expected = i % 2 == 0
                    ? $"DeliveryMode=Persistent; Priority={i}; MessageId=m-{i}; CorrelationId=; Headers=n={i}, x-dotnet-pub-seq-no"
                    : "DeliveryMode=Persistent; Priority=0; MessageId=; CorrelationId=; Headers=x-dotnet-pub-seq-no";

                Assert.Equal(expected, read[$"r{round}-{i}"]);
            }
        }

        static Action<IRabbitProducerBuilder<Shipment>>? Overrides(int i) =>
            i % 2 == 0 ? p => p.MessageId($"m-{i}").Priority((byte)i).Header("n", i) : null;
    }

    private static string DescribeWithoutSequence(BasicProperties properties)
    {
        var headers = string.Join(", ", properties.Headers!.Select(entry =>
            entry.Key == Constants.PublishSequenceNumberHeader ? entry.Key : $"{entry.Key}={entry.Value}"));

        return $"DeliveryMode={properties.DeliveryMode}; Priority={properties.Priority}; MessageId={properties.MessageId}; " +
               $"CorrelationId={properties.CorrelationId}; Headers={headers}";
    }

    /// <summary>A synchronization context with one thread, so every publish and every continuation runs on it.</summary>
    private sealed class OneThread : SynchronizationContext
    {
        private readonly System.Collections.Concurrent.BlockingCollection<(SendOrPostCallback Callback, object? State)> _queue = [];

        public static Task RunAsync(Func<Task> work)
        {
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var context = new OneThread();
            var thread = new Thread(() =>
            {
                SetSynchronizationContext(context);
                context.Post(_ => _ = RunToEndAsync(), null);

                foreach (var (callback, state) in context._queue.GetConsumingEnumerable())
                    callback(state);
            });

            thread.Start();

            return done.Task;

            async Task RunToEndAsync()
            {
                try
                {
                    await work();
                    done.SetResult();
                }
                catch (Exception exception)
                {
                    done.SetException(exception);
                }
                finally
                {
                    context._queue.CompleteAdding();
                }
            }
        }

        public override void Post(SendOrPostCallback d, object? state)
        {
            if (!_queue.IsAddingCompleted)
                _queue.Add((d, state));
        }

        public override void Send(SendOrPostCallback d, object? state) => throw new NotSupportedException();
    }

    private static string Describe(BasicProperties properties)
    {
        var headers = properties.Headers switch
        {
            null => "none",
            { Count: 0 } => "empty",
            { } entries => string.Join(", ", entries.Select(entry => $"{entry.Key}={entry.Value}"))
        };

        return $"DeliveryMode={properties.DeliveryMode}; Priority={properties.Priority}; MessageId={properties.MessageId}; " +
               $"CorrelationId={properties.CorrelationId}; Headers={headers}";
    }

    private RabbitProducer Producer<T>(RabbitOptions options, Action<RabbitProducerDeclaration>? declare = null, bool tracksConfirmations = false)
    {
        var exchange = new RabbitExchangeDeclaration(_connection, "direct", "shipments");
        var declaration = new RabbitProducerDeclaration(typeof(T), _connection, exchange) { RoutingKey = "shipments.created" };
        declare?.Invoke(declaration);
        options.ProducerDeclarations[typeof(T)] = declaration;
        var channel = new RabbitChannel(_connection, _channel.Object, NullLogger<RabbitChannel>.Instance) { TracksConfirmations = tracksConfirmations };
        var channels = new Mock<IRabbitChannelProvider>();
        channels.Setup(c => c.FromDeclarationAsync(It.IsAny<RabbitProducerDeclaration>(), It.IsAny<CancellationToken>())).ReturnsAsync(channel);

        return new RabbitProducer(Options.Create(options), channels.Object, new Mock<IRabbitConnectionProvider>(MockBehavior.Strict).Object, NullLogger<RabbitProducer>.Instance);
    }
}
