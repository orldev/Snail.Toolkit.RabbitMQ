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
            payload = options.Deserializer(body.ToArray(), declaration.PayloadType);
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

        using var scope = serviceProvider.CreateScope();
        try
        {
            foreach (var subscription in declaration.Subscriptions)
            {
                await subscription(scope, payload, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            RabbitTracing.Failed(exception);

            if (declaration.AutoAck)
            {
                RabbitTracing.Settled(RabbitTracing.Settlement.Lost);
                logger.LogError(exception,
                    "Handler failed for a message on queue '{Queue}'; the message was acknowledged on delivery and is lost",
                    queue.Name);
                return;
            }

            await NackFailedAsync(deliveryTag, redelivered, properties, body, exception, cancellationToken);
            return;
        }

        if (!declaration.AutoAck)
        {
            await Channel.BasicAckAsync(
                deliveryTag,
                multiple: false,
                cancellationToken);
        }

        RabbitTracing.Settled(RabbitTracing.Settlement.Acked);
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

        logger.LogError(exception,
            "Poison message on queue '{Queue}': deserialization into '{PayloadType}' failed; the message will not be processed",
            queue.Name, declaration.PayloadType);

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
            logger.LogError(
                "Poison message on queue '{Queue}' dropped: retrying a deterministic deserialization failure is pointless and no parking queue is configured",
                queue.Name);
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
                logger.LogError(exception,
                    "Message on queue '{Queue}' failed after {Attempt} attempt(s); parked to '{ParkingQueue}'",
                    queue.Name, attempt, queue.ParkingQueue.Name);
                await ParkAsync(deliveryTag, properties, body, cancellationToken);
                return;
            }

            logger.LogError(exception,
                "Message on queue '{Queue}' failed after {Attempt} attempt(s); giving up, message dropped",
                queue.Name, attempt);
            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            RabbitTracing.Settled(RabbitTracing.Settlement.Dropped);
            return;
        }

        logger.LogWarning(exception,
            "Message on queue '{Queue}' failed attempt {Attempt}/{MaxAttempts}; scheduled for retry",
            queue.Name, attempt, queue.MaxAttempts);
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
            logger.LogError(exception,
                "Failed to park a message from queue '{Queue}' to '{ParkingQueue}'; the message is rejected for another retry cycle",
                queue.Name, queue.ParkingQueue!.Name);
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
    private int GetAttempt(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is not { } headers ||
            !headers.TryGetValue("x-death", out var header) ||
            header is not IEnumerable deaths)
            return 1;

        foreach (var death in deaths)
        {
            if (death is not IDictionary entry)
                continue;

            if (AsString(entry["queue"]) != queue.Name || AsString(entry["reason"]) != "rejected")
                continue;

            if (entry["count"] is long count)
                return (int)count + 1;
        }

        return 1;
    }

    /// <summary>
    /// Converts an AMQP table value to a string; long strings arrive as UTF-8 byte arrays.
    /// </summary>
    /// <param name="value">The AMQP table value.</param>
    /// <returns>The string value, or null when the value is not string-like.</returns>
    private static string? AsString(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        string text => text,
        _ => null
    };
}
