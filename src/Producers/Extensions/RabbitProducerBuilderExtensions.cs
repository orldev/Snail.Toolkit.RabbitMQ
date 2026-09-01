using System.Globalization;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Producers.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitProducerBuilder{T}"/> to configure RabbitMQ message producers.
/// </summary>
public static class RabbitProducerBuilderExtensions
{
    #region RoutedTo

    /// <summary>
    /// Sets the routing key based on a queue's binding or its name.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="queue">The queue builder to derive the routing key from.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// The routing key will be:
    /// - The binding's routing key if the queue is bound to the producer's exchange
    /// - The queue name if no matching binding exists
    /// </remarks>
    public static IRabbitProducerBuilder<T> RoutedTo<T>(
        this IRabbitProducerBuilder<T> builder,
        IRabbitQueueBuilder<T> queue)
    {
        var binding = queue.Declaration
            .BindingDeclarations
            .FirstOrDefault(b => b.ExchangeDeclaration == builder.ProducerDeclaration.ExchangeDeclaration);

        return builder.RoutedTo(binding?.RoutingKey ?? queue.Declaration.Name);
    }

    /// <summary>
    /// Sets the explicit routing key for messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="routingKey">The routing key to use.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// The routing key determines how messages are routed from the exchange to queues.
    /// For direct exchanges, this typically matches queue names.
    /// </remarks>
    public static IRabbitProducerBuilder<T> RoutedTo<T>(
        this IRabbitProducerBuilder<T> builder,
        string routingKey)
    {
        builder.ProducerDeclaration.RoutingKey = routingKey;
        return builder;
    }

    #endregion

    /// <summary>
    /// Configures messages as mandatory, requiring they be routed to at least one queue.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// If true, unroutable messages will be returned to the producer via BasicReturn.
    /// If false, the server silently drops unroutable messages.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Mandatory<T>(
        this IRabbitProducerBuilder<T> builder)
    {
        builder.ProducerDeclaration.Mandatory = true;
        return builder;
    }

    /// <summary>
    /// Enables transactional mode for the producer channel.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// In transactional mode, published messages are committed only after successful processing.
    /// Requires proper commit/rollback handling in the producer implementation.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Transactional<T>(
        this IRabbitProducerBuilder<T> builder)
    {
        builder.ProducerDeclaration.Transactional = true;
        return builder;
    }
    
    /// <summary>
    /// Configures messages as persistent, ensuring they survive server restarts.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// Persistent messages are written to disk as soon as possible.
    /// Requires queues to also be durable for full message persistence.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Persistent<T>(
        this IRabbitProducerBuilder<T> builder)
    {
        return builder.Property(x => x.Persistent = true);
    }

    /// <summary>
    /// Sets the priority level for messages (clamped to queue's max priority).
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="priority">The priority level (0-255).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// Higher priority messages may be delivered before lower priority messages.
    /// The queue must be configured with x-max-priority to support message priorities.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Priority<T>(
        this IRabbitProducerBuilder<T> builder,
        byte priority)
    {
        return builder.Property(x => x.Priority = priority);
    }

    /// <summary>
    /// Sets the time-to-live for messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="timeToLive">The maximum time the message should live.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// Expired messages will be dead-lettered or discarded.
    /// The expiration time is specified in milliseconds.
    /// </remarks>
    public static IRabbitProducerBuilder<T> MessageTimeToLive<T>(
        this IRabbitProducerBuilder<T> builder,
        TimeSpan timeToLive)
    {
        return builder.Property(x =>
            x.Expiration = ((long)timeToLive.TotalMilliseconds).ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Sets the message identifier for messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="messageId">The application-defined message identifier.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="messageId"/> is null.</exception>
    /// <remarks>
    /// Set a per-message value through the overrides of PublishAsync to enable
    /// consumer-side deduplication of redelivered messages.
    /// </remarks>
    public static IRabbitProducerBuilder<T> MessageId<T>(
        this IRabbitProducerBuilder<T> builder,
        string messageId)
    {
        ArgumentNullException.ThrowIfNull(messageId);
        return builder.Property(p => p.MessageId = messageId);
    }

    /// <summary>
    /// Sets the correlation identifier for messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="correlationId">The identifier correlating this message with a request, task or workflow.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="correlationId"/> is null.</exception>
    /// <remarks>
    /// Set a per-message value through the overrides of PublishAsync to tie
    /// published messages back to the originating operation.
    /// </remarks>
    public static IRabbitProducerBuilder<T> CorrelationId<T>(
        this IRabbitProducerBuilder<T> builder,
        string correlationId)
    {
        ArgumentNullException.ThrowIfNull(correlationId);
        return builder.Property(p => p.CorrelationId = correlationId);
    }

    /// <summary>
    /// Sets the application identifier for messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="appId">The application identifier string.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="appId"/> is null.</exception>
    public static IRabbitProducerBuilder<T> AppId<T>(
        this IRabbitProducerBuilder<T> builder,
        string appId)
    {
        ArgumentNullException.ThrowIfNull(appId);
        return builder.Property(p => p.AppId = appId);
    }

    /// <summary>
    /// Configures a custom message property.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="property">Action to configure the message properties.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// This allows setting any property available on <see cref="IBasicProperties"/>.
    /// Multiple property configurations will be applied in the order they are added.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Property<T>(
        this IRabbitProducerBuilder<T> builder,
        Action<IBasicProperties> property)
    {
        builder.ProducerDeclaration.Properties.Add(property);
        return builder;
    }

    /// <summary>
    /// Adds a custom header to messages.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="header">The header name.</param>
    /// <param name="value">The header value.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="header"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown if the header is already defined.</exception>
    /// <remarks>
    /// Headers can be used for advanced routing with header exchanges.
    /// Header values can be any serializable object.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Header<T>(
        this IRabbitProducerBuilder<T> builder,
        string header,
        object value)
    {
        ArgumentNullException.ThrowIfNull(header);
        return builder.Property(x =>
        {
            x.Headers ??= new Dictionary<string, object?>();
            if (!x.Headers.TryAdd(header, value))
                throw new ArgumentException($"Header '{header}' already registered", nameof(header));
        });
    }

    /// <summary>
    /// Adds a custom argument to the producer declaration.
    /// </summary>
    /// <typeparam name="T">The type of message being produced.</typeparam>
    /// <param name="builder">The producer builder instance.</param>
    /// <param name="argument">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// These arguments may be used by RabbitMQ plugins or for custom exchange implementations.
    /// </remarks>
    public static IRabbitProducerBuilder<T> Argument<T>(
        this IRabbitProducerBuilder<T> builder,
        string argument,
        object value)
    {
        builder.ProducerDeclaration.Arguments.Add(argument, value);
        return builder;
    }
}