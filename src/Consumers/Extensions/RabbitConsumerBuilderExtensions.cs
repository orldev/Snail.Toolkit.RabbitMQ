using Microsoft.Extensions.DependencyInjection;

namespace Snail.Toolkit.RabbitMQ.Consumers.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitConsumerBuilder{T}"/> to configure RabbitMQ message consumers.
/// </summary>
public static class RabbitConsumerBuilderExtensions
{
    #region Subscribe
    
    /// <summary>
    /// Registers a message consumer type that will be resolved from dependency injection for each message.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <typeparam name="TConsumer">The consumer type implementing <see cref="IConsumer{T}"/>.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// The consumer type will be registered as a transient service in the DI container.
    /// A new instance will be created for each message, receiving:
    /// - The deserialized message payload
    /// - A cancellation token for cooperative cancellation
    /// - Full access to scoped services through dependency injection
    /// </remarks>
    public static IRabbitConsumerBuilder<T> Subscribe<T, TConsumer>(
        this IRabbitConsumerBuilder<T> builder)
        where TConsumer : class, IConsumer<T>
    {
        builder.Services.AddTransient<TConsumer>();

        builder.ConsumerDeclaration.Subscriptions.Add(
            async (scope, payload, cancellationToken) =>
            {
                var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
                var message = (T)payload;
                await consumer.HandleAsync(message, cancellationToken);
            });

        return builder;
    }
    
    /// <summary>
    /// Registers an asynchronous message handler with service scope and cancellation support.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="subscription">The asynchronous handler function that receives the message payload.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// The handler receives:
    /// - A service scope for dependency resolution
    /// - The deserialized message payload
    /// - A cancellation token for cooperative cancellation
    /// </remarks>
    public static IRabbitConsumerBuilder<T> Subscribe<T>(
        this IRabbitConsumerBuilder<T> builder,
        Func<IServiceScope, T, CancellationToken, ValueTask> subscription)
    {
        builder.ConsumerDeclaration
            .Subscriptions
            .Add((scope, payload, cancellationToken) => subscription(scope, (T)payload, cancellationToken));

        return builder;
    }

    /// <summary>
    /// Registers an asynchronous message handler with service scope support.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="subscription">The asynchronous handler function that receives the message payload.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Subscribe<T>(
        this IRabbitConsumerBuilder<T> builder,
        Func<IServiceScope, T, ValueTask> subscription)
    {
        return builder.Subscribe((scope, payload, cancellationToken) => subscription(scope, payload));
    }

    /// <summary>
    /// Registers a basic asynchronous message handler.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="subscription">The asynchronous handler function that receives the message payload.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Subscribe<T>(
        this IRabbitConsumerBuilder<T> builder,
        Func<T, ValueTask> subscription)
    {
        return builder.Subscribe((scope, payload, cancellationToken) => subscription(payload));
    }

    #endregion

    /// <summary>
    /// Sets the consumer tag for identifying this consumer.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="tag">The tag to identify the consumer.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the tag is null.</exception>
    public static IRabbitConsumerBuilder<T> Tagged<T>(
        this IRabbitConsumerBuilder<T> builder,
        string tag)
    {
        ArgumentNullException.ThrowIfNull(tag);
        builder.ConsumerDeclaration.Tag = tag;
        return builder;
    }

    /// <summary>
    /// Configures the prefetch count for limiting unacknowledged messages.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="prefetchCount">The maximum number of unacknowledged messages.</param>
    /// <param name="global">Whether the limit is per channel (false) or per connection (true).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Prefetch<T>(
        this IRabbitConsumerBuilder<T> builder,
        ushort prefetchCount,
        bool global = false)
    {
        builder.ConsumerDeclaration.PrefetchCount = prefetchCount;
        builder.ConsumerDeclaration.Global = global;
        return builder;
    }

    /// <summary>
    /// Sets the number of parallel consumers for this configuration.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="count">The number of parallel consumers.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Count<T>(
        this IRabbitConsumerBuilder<T> builder,
        uint count)
    {
        builder.ConsumerDeclaration.Count = count;
        return builder;
    }

    /// <summary>
    /// Configures the consumer as exclusive, preventing other consumers from accessing the queue.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Exclusive<T>(
        this IRabbitConsumerBuilder<T> builder)
    {
        builder.ConsumerDeclaration.Exclusive = true;
        return builder;
    }

    /// <summary>
    /// Enables the no-local flag, preventing the broker from sending messages to the connection that published them.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> NoLocal<T>(this IRabbitConsumerBuilder<T> builder)
    {
        builder.ConsumerDeclaration.NoLocal = true;
        return builder;
    }

    /// <summary>
    /// Enables automatic message acknowledgment upon receipt rather than after processing.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> AutoAck<T>(this IRabbitConsumerBuilder<T> builder)
    {
        builder.ConsumerDeclaration.AutoAck = true;
        return builder;
    }

    /// <summary>
    /// Configures message requeue behavior when consumption fails.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="multiple">Whether to negatively acknowledge multiple messages.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Requeue<T>(
        this IRabbitConsumerBuilder<T> builder,
        bool multiple = false)
    {
        builder.ConsumerDeclaration.Requeue = true;
        builder.ConsumerDeclaration.Multiple = multiple;
        return builder;
    }

    /// <summary>
    /// Sets the consumer priority for message dispatching.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="priority">The priority level (higher values indicate higher priority).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitConsumerBuilder<T> Priority<T>(
        this IRabbitConsumerBuilder<T> builder,
        byte priority)
    {
        return builder.Argument("x-priority", priority);
    }

    /// <summary>
    /// Adds a custom argument to the consumer declaration.
    /// </summary>
    /// <typeparam name="T">The type of message payload to handle.</typeparam>
    /// <param name="builder">The consumer builder instance.</param>
    /// <param name="argument">The name of the argument.</param>
    /// <param name="value">The value of the argument.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the argument name is null.</exception>
    /// <exception cref="ArgumentException">Thrown if the argument is already registered.</exception>
    public static IRabbitConsumerBuilder<T> Argument<T>(
        this IRabbitConsumerBuilder<T> builder,
        string argument,
        object value)
    {
        ArgumentNullException.ThrowIfNull(argument);

        if (!builder.ConsumerDeclaration.Arguments.TryAdd(argument, value))
            throw new ArgumentException($"Argument {argument} already registered", nameof(argument));

        return builder;
    }
}