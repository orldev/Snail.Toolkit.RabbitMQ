using System.Collections;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// Represents a RabbitMQ message consumer that processes incoming messages and dispatches them to registered handlers.
/// </summary>
/// <param name="channel">The RabbitMQ channel to consume messages from.</param>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="serviceProvider">The service provider for resolving dependencies.</param>
/// <param name="queue">The queue declaration this consumer receives messages from.</param>
/// <param name="declaration">The consumer configuration declaration.</param>
/// <param name="logger">The logger for consumer diagnostics.</param>
/// <remarks>
/// This consumer handles:
/// - Message deserialization using the configured deserializer
/// - Dependency scope creation for each message
/// - Error handling and message acknowledgment
/// - Dispatch to multiple subscription handlers
/// - Poison messages: a payload that cannot be deserialized never re-enters the retry cycle,
///   because deserialization is deterministic — it is parked when a parking queue is configured,
///   dropped when the queue retries, and rejected without requeue otherwise
/// - Retry with backoff when the queue is configured via WithRetry, with parking of exhausted messages
/// </remarks>
internal sealed class RabbitConsumer(
    IChannel channel,
    RabbitOptions options,
    IServiceProvider serviceProvider,
    RabbitQueueDeclaration queue,
    RabbitConsumerDeclaration declaration,
    ILogger logger)
    : AsyncDefaultBasicConsumer(channel)
{
    private readonly byte[] _queueName = Encoding.UTF8.GetBytes(queue.Name);

    /// <summary>
    /// Handles an incoming message delivery from RabbitMQ.
    /// </summary>
    /// <param name="consumerTag">The consumer tag associated with the delivery.</param>
    /// <param name="deliveryTag">The delivery tag for the message.</param>
    /// <param name="redelivered">True if the message has been redelivered.</param>
    /// <param name="exchange">The exchange the message was published to.</param>
    /// <param name="routingKey">The routing key used when publishing.</param>
    /// <param name="properties">The message properties (headers, etc.).</param>
    /// <param name="body">The message body as a byte array.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous message processing operation.</returns>
    /// <remarks>
    /// This method will:
    /// 1. Deserialize the message payload using the configured deserializer;
    ///    a poison message (deserializer throws or returns null) never stalls the consumer and never
    ///    cycles through the retry queue — it is parked, dropped or rejected depending on the queue setup
    /// 2. Create a new DI scope for the message processing
    /// 3. Invoke all registered subscription handlers
    /// 4. Handle acknowledgments based on consumer configuration:
    ///    - Auto-acknowledge if configured
    ///    - On failure with WithRetry configured: reject into the retry queue until attempts are exhausted,
    ///      then park the message or acknowledge and drop it
    ///    - On failure otherwise: nack with a single immediate requeue when Requeue is enabled
    ///    - On a cancelled delivery (the host stopping): nack with requeue, so the next consumer takes the message whole
    ///    - Ack on success
    /// </remarks>
    public override async Task HandleBasicDeliverAsync(
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default)
    {
        object? payload;
        try
        {
            payload = options.Deserialize(body, declaration.PayloadType);
        }
        catch (Exception exception)
        {
            await RejectPoisonAsync(deliveryTag, properties, body, exception, cancellationToken);
            return;
        }

        if (payload is null)
        {
            await RejectPoisonAsync(deliveryTag, properties, body, exception: null, cancellationToken);
            return;
        }

        var scope = serviceProvider.CreateScope();
        var subscriptions = declaration.Subscriptions;
        Exception? failure = null;
        try
        {
            for (var i = 0; i < subscriptions.Count; i++)
            {
                await subscriptions[i](scope, payload, cancellationToken);
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            await DisposeAsync(scope);
        }

        if (failure is null)
        {
            await AckAsync(deliveryTag);
            return;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            await ReturnAsync(deliveryTag, failure);
            return;
        }

        RabbitTracing.Failed(failure);

        if (declaration.AutoAck)
        {
            RabbitTracing.Settled(RabbitTracing.Settlement.Lost);
            Log.AutoAckedMessageLost(logger, failure, queue.Name);
            return;
        }

        await NackFailedAsync(deliveryTag, redelivered, properties, body, failure, cancellationToken);
    }

    /// <summary>
    /// Disposes the delivery scope asynchronously when it can be.
    /// </summary>
    /// <param name="scope">The scope the subscriptions ran in.</param>
    /// <returns>A task that completes once the scope's services are disposed.</returns>
    /// <remarks>
    /// A scoped service that is only <see cref="IAsyncDisposable"/> (a database connection, a client of a modern SDK)
    /// makes the container throw on a synchronous dispose, and that throw would come after the acknowledgement. The
    /// scope is kept as the interface the subscriptions take, so no <see cref="AsyncServiceScope"/> is boxed per
    /// delivery, and the handling stays in the delivery's own state machine: a method of its own cost 100 B per
    /// delivery handled later and 70 B per failed one (benchmarks, 2026-10-21).
    /// </remarks>
    private static ValueTask DisposeAsync(IServiceScope scope)
    {
        if (scope is IAsyncDisposable asynchronous)
        {
            return asynchronous.DisposeAsync();
        }

        scope.Dispose();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Acknowledges a handled message.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the handled message.</param>
    /// <remarks>
    /// A channel that died while the handler ran is logged, not thrown: the broker redelivers an unacknowledged
    /// message by itself once the channel is gone, and a throw here would only reach the dispatcher's callback event.
    /// </remarks>
    private async Task AckAsync(ulong deliveryTag)
    {
        if (declaration.AutoAck)
        {
            RabbitTracing.Settled(RabbitTracing.Settlement.Acked);
            return;
        }

        try
        {
            await Channel.BasicAckAsync(deliveryTag, multiple: false);
        }
        catch (Exception exception)
        {
            Log.SettlementFailed(logger, exception, queue.Name);
            return;
        }

        RabbitTracing.Settled(RabbitTracing.Settlement.Acked);
    }

    /// <summary>
    /// Hands a delivery the handler was told to abandon back to the queue, whole, for the next consumer.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the abandoned message.</param>
    /// <param name="failure">The exception the handler ended with, usually the cancellation itself.</param>
    /// <remarks>
    /// A cancelled handler is the host stopping, not the message failing: it must not spend an attempt of the retry
    /// budget and must not be rejected into a dead letter exchange. The settlement itself runs without the cancelled
    /// token, since a settlement is the one thing a shutdown must still finish.
    /// </remarks>
    private async Task ReturnAsync(ulong deliveryTag, Exception failure)
    {
        if (declaration.AutoAck)
        {
            RabbitTracing.Settled(RabbitTracing.Settlement.Lost);
            Log.AutoAckedMessageLost(logger, failure, queue.Name);
            return;
        }

        try
        {
            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: true);
        }
        catch (Exception exception)
        {
            Log.SettlementFailed(logger, exception, queue.Name);
            return;
        }

        RabbitTracing.Settled(RabbitTracing.Settlement.Requeued);
    }

    /// <summary>
    /// Settles a message whose payload could not be deserialized.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the message to settle.</param>
    /// <param name="properties">The message properties to carry into the parking queue.</param>
    /// <param name="body">The message body to carry into the parking queue.</param>
    /// <param name="exception">The deserializer failure, or null when it returned null.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <remarks>
    /// Deserialization is deterministic, so a poison message never enters the retry cycle:
    /// it is parked when a parking queue is configured; acknowledged and dropped when the queue
    /// retries but has no parking queue (a reject there would loop main → retry → main forever);
    /// rejected without requeue otherwise, dead-lettering it when the queue has a dead letter exchange.
    /// </remarks>
    private async Task RejectPoisonAsync(
        ulong deliveryTag,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        Exception? exception,
        CancellationToken cancellationToken)
    {
        RabbitTracing.Poison();
        RabbitTracing.Failed(exception);

        Log.PoisonMessage(logger, exception, queue.Name, declaration.PayloadType);

        if (declaration.AutoAck)
        {
            RabbitTracing.Settled(RabbitTracing.Settlement.Lost);
            return;
        }

        if (queue.ParkingQueue is not null)
        {
            await ParkAsync(deliveryTag, properties, body, cancellationToken);
            return;
        }

        if (queue.RetryQueue is not null)
        {
            Log.PoisonMessageDropped(logger, queue.Name);
            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            RabbitTracing.Settled(RabbitTracing.Settlement.Dropped);
            return;
        }

        await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
        RabbitTracing.Settled(RabbitTracing.Settlement.Rejected);
    }

    /// <summary>
    /// Acknowledges, rejects or parks a message whose handler failed, according to the retry configuration.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the failed message.</param>
    /// <param name="redelivered">True if the message has been redelivered.</param>
    /// <param name="properties">The message properties carrying the x-death header.</param>
    /// <param name="body">The message body to carry into the parking queue.</param>
    /// <param name="exception">The handler failure.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <remarks>
    /// With WithRetry configured on the queue, the message is rejected into the retry queue until
    /// the attempt budget is exhausted, then parked when a parking queue is configured, otherwise
    /// acknowledged, logged and dropped. Without retry the legacy behavior applies: a single
    /// immediate requeue when Requeue is enabled.
    /// </remarks>
    private async Task NackFailedAsync(
        ulong deliveryTag,
        bool redelivered,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (queue.RetryQueue is null)
        {
            var requeue = !redelivered && declaration.Requeue;

            await Channel.BasicNackAsync(
                deliveryTag,
                multiple: false,
                requeue,
                cancellationToken);

            RabbitTracing.Settled(requeue
                ? RabbitTracing.Settlement.Requeued
                : RabbitTracing.Settlement.Rejected);
            return;
        }

        var attempt = GetAttempt(properties);
        RabbitTracing.Attempt(attempt);

        if (attempt >= queue.MaxAttempts)
        {
            if (queue.ParkingQueue is not null)
            {
                Log.MessageParked(logger, exception, queue.Name, attempt, queue.ParkingQueue.Name);
                await ParkAsync(deliveryTag, properties, body, cancellationToken);
                return;
            }

            Log.MessageDropped(logger, exception, queue.Name, attempt);
            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            RabbitTracing.Settled(RabbitTracing.Settlement.Dropped);
            return;
        }

        Log.RetryScheduled(logger, exception, queue.Name, attempt, queue.MaxAttempts);
        await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
        RabbitTracing.Settled(RabbitTracing.Settlement.RetryScheduled);
    }

    /// <summary>
    /// Moves a message into the parking queue and acknowledges the original delivery.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the message being parked.</param>
    /// <param name="properties">The original message properties, preserved for inspection.</param>
    /// <param name="body">The original message body.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <remarks>
    /// The original delivery is acknowledged only after the copy is published, so a publish failure
    /// keeps the message in the broker: it is rejected into the retry cycle and parking is attempted
    /// again on a later delivery.
    /// </remarks>
    private async Task ParkAsync(
        ulong deliveryTag,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        try
        {
            await Channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue.ParkingQueue!.Name,
                mandatory: false,
                basicProperties: new BasicProperties(properties),
                body: body,
                cancellationToken: cancellationToken);
        }
        catch (Exception exception)
        {
            Log.ParkingFailed(logger, exception, queue.Name, queue.ParkingQueue!.Name);
            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
            RabbitTracing.Failed(exception);
            RabbitTracing.Settled(RabbitTracing.Settlement.Rejected);
            return;
        }

        await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
        RabbitTracing.Settled(RabbitTracing.Settlement.Parked);
    }

    /// <summary>
    /// Gets the 1-based number of the current processing attempt from the message's x-death header.
    /// </summary>
    /// <param name="properties">The message properties carrying the x-death header.</param>
    /// <returns>The number of the current attempt; 1 when the message has never been rejected.</returns>
    /// <remarks>
    /// The client delivers the header as a list, read by index so that no enumerator is boxed per failed delivery;
    /// any other sequence is enumerated.
    /// </remarks>
    private int GetAttempt(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is not { } headers ||
            !headers.TryGetValue("x-death", out var header))
            return 1;

        if (header is IList list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                if (GetRejections(list[i]) is { } count)
                    return (int)count + 1;
            }

            return 1;
        }

        if (header is not IEnumerable deaths)
            return 1;

        foreach (var death in deaths)
        {
            if (GetRejections(death) is { } count)
                return (int)count + 1;
        }

        return 1;
    }

    /// <summary>
    /// Gets how often the message was rejected from this consumer's queue, when the x-death entry records that.
    /// </summary>
    /// <param name="death">One entry of the x-death header.</param>
    /// <returns>The count of the entry for this queue and the reason "rejected"; null for any other entry.</returns>
    private long? GetRejections(object? death)
    {
        if (death is not IDictionary entry)
            return null;

        if (!Is(entry["queue"], _queueName, queue.Name) || !Is(entry["reason"], "rejected"u8, "rejected"))
            return null;

        return entry["count"] as long?;
    }

    /// <summary>
    /// Whether an AMQP table value is the given text; long strings arrive as UTF-8 byte arrays, compared as bytes.
    /// </summary>
    /// <param name="value">The AMQP table value.</param>
    /// <param name="utf8">The text encoded as UTF-8.</param>
    /// <param name="text">The text.</param>
    /// <returns><c>true</c> when the value decodes to the text.</returns>
    /// <remarks>
    /// Equal bytes decode to the same text, so the bytes decide without decoding them. Only a text that holds the
    /// replacement character could also be the decoding of bytes that differ (invalid UTF-8); that case is decoded.
    /// </remarks>
    private static bool Is(object? value, ReadOnlySpan<byte> utf8, string text) => value switch
    {
        byte[] bytes => bytes.AsSpan().SequenceEqual(utf8) || (text.Contains('\uFFFD', StringComparison.Ordinal) && Encoding.UTF8.GetString(bytes) == text),
        string other => other == text,
        _ => false
    };
}
