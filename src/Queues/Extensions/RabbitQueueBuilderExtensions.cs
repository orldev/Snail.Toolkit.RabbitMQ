using Snail.Toolkit.RabbitMQ.Exchanges;

namespace Snail.Toolkit.RabbitMQ.Queues.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitQueueBuilder{T}"/> to configure RabbitMQ queue declarations.
/// </summary>
public static class RabbitQueueBuilderExtensions
{
    private static readonly string[] DeadLetteringArguments = ["x-message-ttl", "x-max-length", "x-max-length-bytes"];

    /// <summary>
    /// Configures the queue as durable, meaning it will survive server restarts.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// Durability is mirrored onto the companion retry and parking queues regardless of
    /// whether WithRetry was called before or after this method, so the whole retry
    /// topology always shares one durability setting.
    /// </remarks>
    public static IRabbitQueueBuilder<T> Durable<T>(this IRabbitQueueBuilder<T> builder)
    {
        builder.Declaration.Durable = true;

        if (builder.Declaration.RetryQueue is { } retry)
        {
            retry.Durable = true;
        }

        if (builder.Declaration.ParkingQueue is { } parking)
        {
            parking.Durable = true;
        }

        return builder;
    }

    /// <summary>
    /// Configures the queue as exclusive, meaning it can only be accessed by the current connection.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>Exclusive queues are deleted when the connection that declared them closes.</remarks>
    public static IRabbitQueueBuilder<T> Exclusive<T>(this IRabbitQueueBuilder<T> builder)
    {
        builder.Declaration.Exclusive = true;
        return builder;
    }

    /// <summary>
    /// Configures the queue declaration to not wait for server confirmation.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> NoWait<T>(this IRabbitQueueBuilder<T> builder)
    {
        builder.Declaration.NoWait = true;
        return builder;
    }

    /// <summary>
    /// Configures the queue to be deleted rather than declared, with optional constraints.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="unusedOnly">If true, only delete if the queue has no consumers.</param>
    /// <param name="emptyOnly">If true, only delete if the queue is empty.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> Deleted<T>(
        this IRabbitQueueBuilder<T> builder,
        bool unusedOnly = false,
        bool emptyOnly = false)
    {
        builder.Declaration.Deleted = true;
        builder.Declaration.UnusedOnly = unusedOnly;
        builder.Declaration.EmptyOnly = emptyOnly;

        return builder;
    }

    /// <summary>
    /// Configures the queue as auto-delete, meaning it will be deleted when the last consumer unsubscribes.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> AutoDelete<T>(this IRabbitQueueBuilder<T> builder)
    {
        builder.Declaration.AutoDelete = true;
        return builder;
    }

    /// <summary>
    /// Configures the queue to use lazy mode, which moves messages to disk as early as possible.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>Lazy queues help reduce memory usage by keeping fewer messages in RAM.</remarks>
    public static IRabbitQueueBuilder<T> Lazy<T>(this IRabbitQueueBuilder<T> builder)
    {
        return builder.Argument("x-queue-mode", "lazy");
    }

    /// <summary>
    /// Sets the maximum number of messages the queue can hold.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="count">The maximum number of messages allowed.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the queue retries through <see cref="WithRetry{T}"/>: an overflow would dead-letter messages into the retry queue.</exception>
    /// <seealso cref="RejectPublish{T}"/>
    public static IRabbitQueueBuilder<T> MaxMessageCount<T>(
        this IRabbitQueueBuilder<T> builder,
        uint count)
    {
        RefuseWithRetry(builder.Declaration, "x-max-length");

        return builder.Argument("x-max-length", count);
    }

    /// <summary>
    /// Sets the maximum size (in bytes) the queue can hold.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="bytes">The maximum size in bytes.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the queue retries through <see cref="WithRetry{T}"/>: an overflow would dead-letter messages into the retry queue.</exception>
    public static IRabbitQueueBuilder<T> MaxMessageSize<T>(
        this IRabbitQueueBuilder<T> builder,
        uint bytes)
    {
        RefuseWithRetry(builder.Declaration, "x-max-length-bytes");

        return builder.Argument("x-max-length-bytes", bytes);
    }

    private static IRabbitQueueBuilder<T> DeadLetterTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilderCore exchange,
        string? routingKey)
    {
        ArgumentNullException.ThrowIfNull(exchange);

        if (routingKey is not null)
        {
            builder.Argument("x-dead-letter-routing-key", routingKey);
        }

        return builder.Argument("x-dead-letter-exchange", exchange.ExchangeDeclaration.Name);
    }

    /// <summary>
    /// Configures a dead letter exchange and optional routing key for rejected or expired messages.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange where dead letters should be sent.</param>
    /// <param name="routingKey">Optional routing key for dead letters.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> DeadLetterTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder<T> exchange,
        string? routingKey = null)
    {
        return builder.DeadLetterTo((IRabbitExchangeBuilderCore)exchange, routingKey);
    }

    /// <summary>
    /// Configures a dead letter exchange and optional routing key for rejected or expired messages.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange where dead letters should be sent.</param>
    /// <param name="routingKey">Optional routing key for dead letters.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> DeadLetterTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder exchange,
        string? routingKey = null)
    {
        return builder.DeadLetterTo((IRabbitExchangeBuilderCore)exchange, routingKey);
    }

    /// <summary>
    /// Configures a dead letter exchange using the same routing key as the queue's binding.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange where dead letters should be sent.</param>
    /// <param name="queue">The queue whose binding provides the routing key.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> DeadLetterTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder<T> exchange,
        IRabbitQueueBuilder<T> queue)
    {
        var binding = queue.Declaration
            .BindingDeclarations
            .FirstOrDefault(b => b.ExchangeDeclaration == exchange.ExchangeDeclaration);

        return builder.DeadLetterTo(exchange, binding?.RoutingKey ?? queue.Declaration.Name);
    }

    /// <summary>
    /// Configures a dead letter exchange using the same routing key as the queue's binding.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange where dead letters should be sent.</param>
    /// <param name="queue">The queue whose binding provides the routing key.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> DeadLetterTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder exchange,
        IRabbitQueueBuilder<T> queue)
    {
        var binding = queue.Declaration
            .BindingDeclarations
            .FirstOrDefault(b => b.ExchangeDeclaration == exchange.ExchangeDeclaration);

        return builder.DeadLetterTo(exchange, binding?.RoutingKey ?? queue.Declaration.Name);
    }

    /// <summary>
    /// Configures the queue to expire after a specified time period of inactivity.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="timeToLive">The inactivity period after which the queue will be deleted.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the duration does not fit the broker's 32-bit milliseconds.</exception>
    public static IRabbitQueueBuilder<T> TimeToLive<T>(
        this IRabbitQueueBuilder<T> builder,
        TimeSpan timeToLive)
    {
        return builder.Argument("x-expires", Milliseconds(timeToLive, nameof(timeToLive)));
    }

    /// <summary>
    /// Configures the default time-to-live for messages in this queue.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="timeToLive">The time after which messages will expire.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the queue retries through <see cref="WithRetry{T}"/>: an expired message would dead-letter into the retry queue.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the duration does not fit the broker's 32-bit milliseconds.</exception>
    public static IRabbitQueueBuilder<T> MessageTimeToLive<T>(
        this IRabbitQueueBuilder<T> builder,
        TimeSpan timeToLive)
    {
        RefuseWithRetry(builder.Declaration, "x-message-ttl");

        return builder.Argument("x-message-ttl", Milliseconds(timeToLive, nameof(timeToLive)));
    }

    /// <summary>
    /// Configures the maximum priority level for messages in this queue.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="priority">The maximum priority value (0-255).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> MaxPriority<T>(
        this IRabbitQueueBuilder<T> builder,
        byte priority)
    {
        return builder.Argument("x-max-priority", priority);
    }

    /// <summary>
    /// Configures the queue to reject new messages when full rather than dropping messages from the head.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> RejectPublish<T>(
        this IRabbitQueueBuilder<T> builder)
    {
        return builder.Argument("x-overflow", "reject-publish");
    }

    private static IRabbitQueueBuilder<T> BoundTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilderCore exchange,
        Action<IRabbitQueueBindingBuilder>? binding)
    {
        ArgumentNullException.ThrowIfNull(exchange);

        var declaration = new RabbitQueueBindingDeclaration(exchange.ExchangeDeclaration);
        binding?.Invoke(new RabbitQueueBindingBuilder(declaration));
        builder.Declaration.AddBinding(declaration);

        return builder;
    }

    /// <summary>
    /// Binds the queue to an exchange with optional binding configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange to bind to.</param>
    /// <param name="binding">Optional action to configure the binding.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> BoundTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder exchange,
        Action<IRabbitQueueBindingBuilder>? binding = null)
    {
        return builder.BoundTo((IRabbitExchangeBuilderCore)exchange, binding);
    }

    /// <summary>
    /// Binds the queue to an exchange with optional binding configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="exchange">The exchange to bind to.</param>
    /// <param name="binding">Optional action to configure the binding.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> BoundTo<T>(
        this IRabbitQueueBuilder<T> builder,
        IRabbitExchangeBuilder<T> exchange,
        Action<IRabbitQueueBindingBuilder>? binding = null)
    {
        return builder.BoundTo((IRabbitExchangeBuilderCore)exchange, binding);
    }

    /// <summary>
    /// Configures retry with backoff: failed messages are parked in a companion
    /// "{queue}.retry" queue for the given delay and then returned to this queue.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="delay">How long a rejected message waits before redelivery.</param>
    /// <param name="maxAttempts">Total processing attempts per message; once exhausted the message is parked, or acknowledged and dropped when no parking queue is configured.</param>
    /// <param name="parkTo">Optional name of a parking queue that keeps exhausted and poison messages for manual inspection instead of dropping them.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a dead letter exchange, a message time to live or a length limit is already configured on the queue.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the delay does not fit the broker's 32-bit milliseconds.</exception>
    /// <remarks>
    /// The queue's dead letter exchange is pointed at the companion retry queue via the default exchange,
    /// so WithRetry cannot be combined with <see cref="DeadLetterTo{T}(IRabbitQueueBuilder{T}, IRabbitExchangeBuilder{T}, string?)"/>.
    /// The broker takes that road for every dead-lettering reason, so it cannot be combined with
    /// <see cref="MessageTimeToLive{T}"/>, <see cref="MaxMessageCount{T}"/> or <see cref="MaxMessageSize{T}"/> either:
    /// an expired or overflowed message would travel main → retry → main without end, because only a rejection
    /// counts as an attempt.
    /// The companion queues are declared automatically alongside this queue and mirror its durability.
    /// Poison messages bypass the retry cycle: a deserialization failure is deterministic, so they are
    /// parked immediately, or dropped when no parking queue is configured.
    /// </remarks>
    public static IRabbitQueueBuilder<T> WithRetry<T>(
        this IRabbitQueueBuilder<T> builder,
        TimeSpan delay,
        int maxAttempts = 3,
        string? parkTo = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(delay, TimeSpan.Zero);

        if (builder.Declaration.Arguments.ContainsKey("x-dead-letter-exchange"))
            throw new InvalidOperationException(
                $"Queue '{builder.Declaration.Name}' already has a dead letter exchange configured; WithRetry cannot be combined with DeadLetterTo");

        foreach (var argument in DeadLetteringArguments)
        {
            if (builder.Declaration.Arguments.ContainsKey(argument))
                throw new InvalidOperationException(
                    $"Queue '{builder.Declaration.Name}' has '{argument}' configured; WithRetry would dead-letter expired or overflowed messages into the retry queue forever");
        }

        var retryName = $"{builder.Declaration.Name}.retry";
        var retry = new RabbitQueueDeclaration(builder.Declaration.ConnectionDeclaration, retryName)
        {
            Durable = builder.Declaration.Durable
        };
        retry.Arguments.Add("x-message-ttl", Milliseconds(delay, nameof(delay)));
        retry.Arguments.Add("x-dead-letter-exchange", string.Empty);
        retry.Arguments.Add("x-dead-letter-routing-key", builder.Declaration.Name);

        builder.Declaration.RetryQueue = retry;
        builder.Declaration.MaxAttempts = maxAttempts;

        if (parkTo is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(parkTo);

            builder.Declaration.ParkingQueue = new RabbitQueueDeclaration(
                builder.Declaration.ConnectionDeclaration, parkTo)
            {
                Durable = builder.Declaration.Durable
            };
        }

        return builder
            .Argument("x-dead-letter-exchange", string.Empty)
            .Argument("x-dead-letter-routing-key", retryName);
    }

    /// <summary>
    /// The whole milliseconds of a duration, refused when they do not fit the 32-bit value the broker reads.
    /// </summary>
    /// <param name="duration">The duration to convert.</param>
    /// <param name="argument">The name of the caller's parameter, for the exception.</param>
    /// <returns>The milliseconds as the broker's integer.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the milliseconds exceed <see cref="int.MaxValue"/>.</exception>
    /// <remarks>
    /// A double-to-int cast saturates on this runtime: WithRetry(TimeSpan.FromDays(30)) silently became a retry after
    /// 24.85 days (chaos test, 2026-10-21).
    /// </remarks>
    private static int Milliseconds(TimeSpan duration, string argument)
    {
        var milliseconds = (long)duration.TotalMilliseconds;
        ArgumentOutOfRangeException.ThrowIfGreaterThan(milliseconds, int.MaxValue, argument);

        return (int)milliseconds;
    }

    /// <summary>
    /// Refuses an argument that would dead-letter messages into the retry queue for a reason the retry never counts.
    /// </summary>
    /// <param name="declaration">The queue being configured.</param>
    /// <param name="argument">The queue argument about to be set.</param>
    /// <exception cref="InvalidOperationException">Thrown when the queue retries through its dead letter exchange.</exception>
    /// <remarks>
    /// The broker takes one dead letter road per queue whatever the reason. An expired or overflowed message on a
    /// retrying queue would travel main → retry → main without end, because only a rejection counts as an attempt.
    /// </remarks>
    private static void RefuseWithRetry(RabbitQueueDeclaration declaration, string argument)
    {
        if (declaration.RetryQueue is null)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Queue '{declaration.Name}' retries through its dead letter exchange; '{argument}' would dead-letter expired or overflowed messages into the retry queue forever");
    }

    /// <summary>
    /// Configures the queue as a quorum queue with optional initial group size.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="initialGroupSize">Optional initial quorum group size.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>Quorum queues provide data safety by replicating messages across multiple nodes.</remarks>
    public static IRabbitQueueBuilder<T> Quorum<T>(
        this IRabbitQueueBuilder<T> builder,
        int? initialGroupSize = null)
    {
        builder.Argument("x-queue-type", "quorum");

        if (initialGroupSize is not null)
        {
            builder.Argument("x-quorum-initial-group-size", initialGroupSize);
        }

        return builder;
    }

    /// <summary>
    /// Configures the queue to have only one active consumer at a time.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitQueueBuilder<T> SingleActiveConsumer<T>(
        this IRabbitQueueBuilder<T> builder)
    {
        return builder.Argument("x-single-active-consumer", true);
    }

    /// <summary>
    /// Adds a custom argument to the queue declaration.
    /// </summary>
    /// <typeparam name="T">The type associated with the queue.</typeparam>
    /// <param name="builder">The queue builder instance.</param>
    /// <param name="argument">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the argument is already configured on the queue.</exception>
    /// <remarks>
    /// This method allows setting any RabbitMQ queue argument not explicitly covered by other methods.
    /// A duplicate argument fails with a message naming the queue and the argument, instead of the
    /// dictionary's generic duplicate-key error surfacing from deep inside a builder chain.
    /// </remarks>
    public static IRabbitQueueBuilder<T> Argument<T>(
        this IRabbitQueueBuilder<T> builder,
        string argument,
        object value)
    {
        if (!builder.Declaration.Arguments.TryAdd(argument, value))
            throw new InvalidOperationException(
                $"Queue '{builder.Declaration.Name}' already has the '{argument}' argument configured");

        return builder;
    }
}
