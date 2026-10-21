using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>
/// A channel that accepts every publish and settlement at once and counts them; no broker, no socket. What a benchmark
/// does not touch throws, so a path that starts using more of the channel shows at once.
/// </summary>
/// <remarks>
/// On a channel opened with publisher confirmations it does to the properties what RabbitMQ.Client 7.2 does before it
/// writes them: properties without headers get a new dictionary, and the publish sequence number is added to the
/// headers. What the library hands the client decides what the client allocates there, so the fake allocates it too.
/// </remarks>
public sealed class FakeChannel : IChannel
{
    public long PublishedBytes { get; private set; }

    /// <summary>
    /// Called with every published body and its properties, as the client would write them, while both are still
    /// valid; null in a timed run, so it costs nothing there.
    /// </summary>
    public Action<ReadOnlyMemory<byte>, IReadOnlyBasicProperties>? Received { get; init; }

    /// <summary>Whether the channel was opened with publisher confirmations; set by the connection that opens it.</summary>
    public bool TracksConfirmations { get; set; }

    /// <summary>Called with every settlement: the delivery tag, whether it was an ack, and whether a nack requeued.</summary>
    public Action<ulong, bool, bool>? Settling { get; init; }

    private ulong _sequence;

    /// <summary>Whether a publish answers with a confirm still to come, as a broker does, instead of one already there.</summary>
    public bool ConfirmsLater { get; init; }

    private PendingConfirm Confirm { get; } = new();

    /// <summary>Sends the confirm of the last publish, when <see cref="ConfirmsLater"/> held it back.</summary>
    public void Acknowledge() => Confirm.Arrive();

    public long Settled { get; private set; }

    public int ChannelNumber => 1;

    public bool IsOpen => true;

    public ValueTask BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        PublishedBytes += body.Length;
        AddSequenceNumber(basicProperties);
        Received?.Invoke(body, basicProperties);

        return ConfirmsLater ? Confirm.Next() : ValueTask.CompletedTask;
    }

    public ValueTask BasicPublishAsync<TProperties>(CachedString exchange, CachedString routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        where TProperties : IReadOnlyBasicProperties, IAmqpHeader
    {
        PublishedBytes += body.Length;
        AddSequenceNumber(basicProperties);
        Received?.Invoke(body, basicProperties);

        return ConfirmsLater ? Confirm.Next() : ValueTask.CompletedTask;
    }

    public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken = default)
    {
        Settled++;
        Settling?.Invoke(deliveryTag, true, false);

        return ValueTask.CompletedTask;
    }

    public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken = default)
    {
        Settled++;
        Settling?.Invoke(deliveryTag, false, requeue);

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// What the client's publish does to the headers on a channel with publisher confirmations: a new dictionary when
    /// the properties carry none, then the sequence number of the publish.
    /// </summary>
    private void AddSequenceNumber<TProperties>(TProperties basicProperties)
        where TProperties : IReadOnlyBasicProperties
    {
        if (!TracksConfirmations)
            return;

        var headers = basicProperties.Headers;

        if (headers is null)
        {
            headers = new Dictionary<string, object?>();

            if (basicProperties is BasicProperties writable)
                writable.Headers = headers;
        }

        headers[Constants.PublishSequenceNumberHeader] = (long)++_sequence;
    }

    public ValueTask BasicRejectAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ValueTask<ulong> GetNextPublishSequenceNumberAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task BasicCancelAsync(string consumerTag, bool noWait = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Task<string> BasicConsumeAsync(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object?>? arguments, IAsyncBasicConsumer consumer, CancellationToken cancellationToken = default) => throw new NotSupportedException();

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
