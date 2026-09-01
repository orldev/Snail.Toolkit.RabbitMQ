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
/// - Poison messages: a payload that cannot be deserialized is rejected without requeue
///   (dead-lettered when the queue has a dead letter exchange), so the consumer keeps running
/// - Retry with backoff when the queue is configured via WithRetry
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
    ///    a poison message (deserializer throws or returns null) is rejected without requeue and never stalls the consumer
    /// 2. Create a new DI scope for the message processing
    /// 3. Invoke all registered subscription handlers
    /// 4. Handle acknowledgments based on consumer configuration:
    ///    - Auto-acknowledge if configured
    ///    - On failure with WithRetry configured: reject into the retry queue until attempts are exhausted, then ack and drop
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
            logger.LogError(exception,
                "Poison message on queue '{Queue}': deserialization into '{PayloadType}' failed; message rejected without requeue",
                queue.Name, declaration.PayloadType);
            await RejectAsync(deliveryTag, cancellationToken);
            return;
        }

        if (payload is null)
        {
            logger.LogError(
                "Poison message on queue '{Queue}': deserialization into '{PayloadType}' returned null; message rejected without requeue",
                queue.Name, declaration.PayloadType);
            await RejectAsync(deliveryTag, cancellationToken);
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
            if (!declaration.AutoAck)
            {
                await NackFailedAsync(deliveryTag, redelivered, properties, exception, cancellationToken);
            }

            throw;
        }

        if (!declaration.AutoAck)
        {
            await Channel.BasicAckAsync(
                deliveryTag,
                declaration.Multiple,
                cancellationToken);
        }
    }

    /// <summary>
    /// Rejects a message without requeue, dead-lettering it when the queue has a dead letter exchange.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the message to reject.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    private async Task RejectAsync(ulong deliveryTag, CancellationToken cancellationToken)
    {
        if (!declaration.AutoAck)
        {
            await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
        }
    }

    /// <summary>
    /// Acknowledges or rejects a message whose handler failed, according to the retry configuration.
    /// </summary>
    /// <param name="deliveryTag">The delivery tag of the failed message.</param>
    /// <param name="redelivered">True if the message has been redelivered.</param>
    /// <param name="properties">The message properties carrying the x-death header.</param>
    /// <param name="exception">The handler failure.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <remarks>
    /// With WithRetry configured on the queue, the message is rejected into the retry queue until
    /// the attempt budget is exhausted, then acknowledged, logged and dropped. Without retry the
    /// legacy behavior applies: a single immediate requeue when Requeue is enabled.
    /// </remarks>
    private async Task NackFailedAsync(
        ulong deliveryTag,
        bool redelivered,
        IReadOnlyBasicProperties properties,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (queue.RetryQueue is null)
        {
            await Channel.BasicNackAsync(
                deliveryTag,
                declaration.Multiple,
                !redelivered && declaration.Requeue,
                cancellationToken);
            return;
        }

        var attempt = GetAttempt(properties);
        if (attempt >= queue.MaxAttempts)
        {
            logger.LogError(exception,
                "Message on queue '{Queue}' failed after {Attempt} attempt(s); giving up, message dropped",
                queue.Name, attempt);
            await Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            return;
        }

        logger.LogWarning(exception,
            "Message on queue '{Queue}' failed attempt {Attempt}/{MaxAttempts}; scheduled for retry",
            queue.Name, attempt, queue.MaxAttempts);
        await Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
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
