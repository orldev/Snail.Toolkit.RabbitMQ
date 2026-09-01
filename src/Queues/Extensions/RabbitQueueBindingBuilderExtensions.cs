namespace Snail.Toolkit.RabbitMQ.Queues.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitQueueBindingBuilder"/> to configure RabbitMQ queue bindings.
/// </summary>
public static class RabbitQueueBindingBuilderExtensions
{
    /// <summary>
    /// Configures the routing key for the queue binding.
    /// </summary>
    /// <param name="builder">The binding builder instance.</param>
    /// <param name="routingKey">The routing key that determines which messages are routed from the exchange to the queue.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// The routing key acts as a filter - only messages published with a matching routing key will be delivered to the queue.
    /// For fanout exchanges, this value is typically ignored.
    /// </remarks>
    public static IRabbitQueueBindingBuilder RoutedTo(
        this IRabbitQueueBindingBuilder builder,
        string routingKey)
    {
        builder.Declaration.RoutingKey = routingKey;
        return builder;
    }

    /// <summary>
    /// Configures the binding operation to not wait for server confirmation.
    /// </summary>
    /// <param name="builder">The binding builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// When set to true, the server will not send an acknowledgement for the binding operation.
    /// </remarks>
    public static IRabbitQueueBindingBuilder NoWait(this IRabbitQueueBindingBuilder builder)
    {
        builder.Declaration.NoWait = true;
        return builder;
    }

    /// <summary>
    /// Configures the binding to be removed rather than created.
    /// </summary>
    /// <param name="builder">The binding builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// When set to true, this will unbind the queue from the exchange rather than creating the binding.
    /// </remarks>
    public static IRabbitQueueBindingBuilder Deleted(this IRabbitQueueBindingBuilder builder)
    {
        builder.Declaration.Deleted = true;
        return builder;
    }

    /// <summary>
    /// Adds a custom argument to the binding declaration.
    /// </summary>
    /// <typeparam name="T">The type of the argument value.</typeparam>
    /// <param name="builder">The binding builder instance.</param>
    /// <param name="argument">The name of the argument to add.</param>
    /// <param name="value">The value of the argument.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the value parameter is null.</exception>
    /// <remarks>
    /// This method allows setting any RabbitMQ binding argument not explicitly covered by other methods.
    /// Common binding arguments include headers for header exchanges or other exchange-specific parameters.
    /// </remarks>
    public static IRabbitQueueBindingBuilder Argument<T>(
        this IRabbitQueueBindingBuilder builder,
        string argument,
        T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        builder.Declaration.Arguments.Add(argument, value);
        return builder;
    }
}