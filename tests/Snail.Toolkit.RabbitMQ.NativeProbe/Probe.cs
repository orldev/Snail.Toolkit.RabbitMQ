using System.Text.Json.Serialization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.NativeProbe;

/// <summary>The message the probe publishes and consumes.</summary>
public sealed record Order(string Id, int Quantity, decimal Total);

/// <summary>The source-generated JSON a native application passes to UseSerialization.</summary>
[JsonSerializable(typeof(Order))]
public sealed partial class ProbeJson : JsonSerializerContext;

/// <summary>The channel provider once a channel is open: no broker behind it.</summary>
public sealed class OpenChannel(RabbitChannel channel) : IRabbitChannelProvider
{
    public Task<RabbitChannel> FromDeclarationAsync(RabbitProducerDeclaration declaration, CancellationToken cancellationToken = default) =>
        Task.FromResult(channel);
}

/// <summary>
/// A channel that keeps what was published and hands it back to the consumer that subscribed; no broker, no socket.
/// </summary>
public sealed class FakeChannel : IChannel
{
    private IAsyncBasicConsumer? _consumer;

    public ReadOnlyMemory<byte> Body { get; private set; }

    public int Acknowledged { get; private set; }

    public int ChannelNumber => 1;

    public bool IsOpen => true;

    public Task DeliverAsync() =>
        _consumer!.HandleBasicDeliverAsync("probe", 1, false, string.Empty, "orders", new BasicProperties(), Body);

    public ValueTask BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        Body = body.ToArray();

        return ValueTask.CompletedTask;
    }

    public ValueTask BasicPublishAsync<TProperties>(CachedString exchange, CachedString routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        Body = body.ToArray();

        return ValueTask.CompletedTask;
    }

    public Task<string> BasicConsumeAsync(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object?>? arguments, IAsyncBasicConsumer consumer, CancellationToken cancellationToken = default)
    {
        _consumer = consumer;

        return Task.FromResult("probe");
    }

    public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken = default)
    {
        Acknowledged++;

        return ValueTask.CompletedTask;
    }

    public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ValueTask BasicRejectAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ValueTask<ulong> GetNextPublishSequenceNumberAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task BasicCancelAsync(string consumerTag, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<BasicGetResult?> BasicGetAsync(string queue, bool autoAck, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task CloseAsync(ushort replyCode, string replyText, bool abort, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task CloseAsync(ShutdownEventArgs reason, bool abort) => throw new NotSupportedException();

    public Task CloseAsync(ShutdownEventArgs reason, bool abort, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object?>? arguments = null, bool passive = false, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task ExchangeDeclarePassiveAsync(string exchange, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task ExchangeDeleteAsync(string exchange, bool ifUnused = false, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task ExchangeBindAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments = null, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task ExchangeUnbindAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments = null, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<QueueDeclareOk> QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, bool passive = false, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<QueueDeclareOk> QueueDeclarePassiveAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<uint> QueueDeleteAsync(string queue, bool ifUnused, bool ifEmpty, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<uint> QueuePurgeAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task QueueBindAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments = null, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task QueueUnbindAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<uint> MessageCountAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<uint> ConsumerCountAsync(string queue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task TxCommitAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task TxRollbackAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task TxSelectAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ShutdownEventArgs? CloseReason => null;

    public IAsyncBasicConsumer? DefaultConsumer { get; set; }

    public bool IsClosed => false;

    public string? CurrentQueue => null;

    public TimeSpan ContinuationTimeout { get; set; }

    public event AsyncEventHandler<BasicAckEventArgs> BasicAcksAsync { add { } remove { } }

    public event AsyncEventHandler<BasicNackEventArgs> BasicNacksAsync { add { } remove { } }

    public event AsyncEventHandler<BasicReturnEventArgs> BasicReturnAsync { add { } remove { } }

    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync { add { } remove { } }

    public event AsyncEventHandler<FlowControlEventArgs> FlowControlAsync { add { } remove { } }

    public event AsyncEventHandler<ShutdownEventArgs> ChannelShutdownAsync { add { } remove { } }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
    }
}
