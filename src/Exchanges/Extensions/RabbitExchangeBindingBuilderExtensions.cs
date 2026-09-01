namespace Snail.Toolkit.RabbitMQ.Exchanges.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitExchangeBindingBuilder"/> to configure RabbitMQ exchange bindings.
/// </summary>
public static class RabbitExchangeBindingBuilderExtensions
{
    /// <summary>
    /// Configures the routing key for the exchange binding.
    /// </summary>
    /// <param name="builder">The binding builder instance.</param>
    /// <param name="routingKey">The routing key that determines how messages are routed between exchanges.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// For direct and topic exchanges, this specifies the exact or pattern-based routing key.
    /// For fanout exchanges, this parameter is typically ignored.
    /// </remarks>
    public static IRabbitExchangeBindingBuilder RoutedTo(
        this IRabbitExchangeBindingBuilder builder,
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
    /// When enabled, the server will not send an acknowledgement for the binding operation.
    /// This can improve performance but reduces reliability.
    /// </remarks>
    public static IRabbitExchangeBindingBuilder NoWait(
        this IRabbitExchangeBindingBuilder builder)
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
    /// When set to true, this will unbind the exchanges rather than creating the binding.
    /// </remarks>
    public static IRabbitExchangeBindingBuilder Deleted(
        this IRabbitExchangeBindingBuilder builder)
    {
        builder.Declaration.Deleted = true;
        return builder;
    }

    /// <summary>
    /// Adds a custom argument to the binding declaration.
    /// </summary>
    /// <typeparam name="TValue">The type of the argument value.</typeparam>
    /// <param name="builder">The binding builder instance.</param>
    /// <param name="argument">The name of the argument.</param>
    /// <param name="value">The value of the argument.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the value is null.</exception>
    /// <remarks>
    /// These arguments can be used for exchange-specific features or plugins.
    /// Common arguments include alternate-exchange, headers matching, etc.
    /// </remarks>
    public static IRabbitExchangeBindingBuilder Argument<TValue>(
        this IRabbitExchangeBindingBuilder builder,
        string argument,
        TValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        builder.Declaration.Arguments.Add(argument, value);
        return builder;
    }
}