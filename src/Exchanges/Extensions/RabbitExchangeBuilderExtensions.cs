namespace Snail.Toolkit.RabbitMQ.Exchanges.Extensions;

/// <summary>
/// Provides extension methods for configuring RabbitMQ exchanges and their bindings.
/// </summary>
public static class RabbitExchangeBuilderExtensions
{
    /// <summary>
    /// Configures the exchange as durable, meaning it will survive server restarts.
    /// </summary>
    /// <typeparam name="T">The builder type implementing <see cref="IRabbitExchangeBuilderCore"/>.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static T Durable<T>(
        this T builder)
        where T : IRabbitExchangeBuilderCore
    {
        builder.ExchangeDeclaration.Durable = true;
        return builder;
    }

    /// <summary>
    /// Configures the exchange declaration to not wait for server confirmation.
    /// </summary>
    /// <typeparam name="T">The builder type implementing <see cref="IRabbitExchangeBuilderCore"/>.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static T NoWait<T>(
        this T builder)
        where T : IRabbitExchangeBuilderCore
    {
        builder.ExchangeDeclaration.NoWait = true;
        return builder;
    }
    
    /// <summary>
    /// Configures the exchange as auto-delete, meaning it will be removed when no longer in use.
    /// </summary>
    /// <typeparam name="T">The builder type implementing <see cref="IRabbitExchangeBuilderCore"/>.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static T AutoDelete<T>(
        this T builder)
        where T : IRabbitExchangeBuilderCore
    {
        builder.ExchangeDeclaration.AutoDelete = true;
        return builder;
    }

    /// <summary>
    /// Configures the exchange to be deleted rather than declared.
    /// </summary>
    /// <typeparam name="T">The builder type implementing <see cref="IRabbitExchangeBuilderCore"/>.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="unusedOnly">If true, only delete if the exchange is unused.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static T Deleted<T>(
        this T builder,
        bool unusedOnly = false)
        where T : IRabbitExchangeBuilderCore
    {
        builder.ExchangeDeclaration.Deleted = true;
        builder.ExchangeDeclaration.UnusedOnly = unusedOnly;
        return builder;
    }

    /// <summary>
    /// Adds a custom argument to the exchange declaration.
    /// </summary>
    /// <typeparam name="T">The builder type implementing <see cref="IRabbitExchangeBuilderCore"/>.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="argument">The name of the argument.</param>
    /// <param name="value">The value of the argument.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static T Argument<T>(
        this T builder,
        string argument,
        object value)
        where T : IRabbitExchangeBuilderCore
    {
        builder.ExchangeDeclaration.Arguments.Add(argument, value);
        return builder;
    }

    #region BoundTo

    private static T BoundTo<T>(
        this T builder,
        IRabbitExchangeBuilderCore exchange,
        Action<IRabbitExchangeBindingBuilder>? binding)
        where T : IRabbitExchangeBuilderCore
    {
        ArgumentNullException.ThrowIfNull(exchange);
        
        var declaration = new RabbitExchangeBindingDeclaration(exchange.ExchangeDeclaration);
        binding?.Invoke(new RabbitExchangeBindingBuilder(declaration));
        builder.ExchangeDeclaration.BindingDeclarations.Add(declaration);
        return builder;
    }

    /// <summary>
    /// Binds the exchange to another exchange with optional binding configuration.
    /// </summary>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The target exchange to bind to.</param>
    /// <param name="binding">Optional action to configure the binding.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitExchangeBuilder BoundTo(
        this IRabbitExchangeBuilder builder,
        IRabbitExchangeBuilderCore exchange,
        Action<IRabbitExchangeBindingBuilder>? binding = null)
    {
        return builder.BoundTo<IRabbitExchangeBuilder>(exchange, binding);
    }

    /// <summary>
    /// Binds the typed exchange to another typed exchange with optional binding configuration.
    /// </summary>
    /// <typeparam name="T">The message type associated with the exchange.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The target exchange to bind to.</param>
    /// <param name="binding">Optional action to configure the binding.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitExchangeBuilder<T> BoundTo<T>(
        this IRabbitExchangeBuilder<T> builder,
        IRabbitExchangeBuilder<T> exchange,
        Action<IRabbitExchangeBindingBuilder>? binding = null)
    {
        return builder.BoundTo<IRabbitExchangeBuilder<T>>(exchange, binding);
    }

    /// <summary>
    /// Binds the typed exchange to an untyped exchange with optional binding configuration.
    /// </summary>
    /// <typeparam name="T">The message type associated with the exchange.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The target exchange to bind to.</param>
    /// <param name="binding">Optional action to configure the binding.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitExchangeBuilder<T> BoundTo<T>(
        this IRabbitExchangeBuilder<T> builder,
        IRabbitExchangeBuilder exchange,
        Action<IRabbitExchangeBindingBuilder>? binding = null)
    {
        return builder.BoundTo<IRabbitExchangeBuilder<T>>(exchange, binding);
    }

    #endregion

    #region AlternateTo

    private static T AlternateTo<T>(
        this T builder,
        IRabbitExchangeBuilderCore exchange)
        where T : IRabbitExchangeBuilderCore
    {
        ArgumentNullException.ThrowIfNull(exchange);
        return builder.Argument("alternate-exchange", exchange.ExchangeDeclaration.Name);
    }
    
    /// <summary>
    /// Configures an alternate exchange for unroutable messages.
    /// </summary>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The alternate exchange for unroutable messages.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// Messages that cannot be routed will be forwarded to the specified alternate exchange
    /// instead of being discarded or marked as dead.
    /// </remarks>
    public static IRabbitExchangeBuilder AlternateTo(
        this IRabbitExchangeBuilder builder,
        IRabbitExchangeBuilderCore exchange)
    {
        return builder.AlternateTo<IRabbitExchangeBuilder>(exchange);
    }

    /// <summary>
    /// Configures a typed alternate exchange for unroutable messages.
    /// </summary>
    /// <typeparam name="T">The message type associated with the exchange.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The alternate exchange for unroutable messages.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitExchangeBuilder<T> AlternateTo<T>(
        this IRabbitExchangeBuilder<T> builder,
        IRabbitExchangeBuilder<T> exchange)
    {
        return builder.AlternateTo<IRabbitExchangeBuilder<T>>(exchange);
    }
    
    /// <summary>
    /// Configures an untyped alternate exchange for typed exchange's unroutable messages.
    /// </summary>
    /// <typeparam name="T">The message type associated with the exchange.</typeparam>
    /// <param name="builder">The exchange builder instance.</param>
    /// <param name="exchange">The alternate exchange for unroutable messages.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public static IRabbitExchangeBuilder<T> AlternateTo<T>(
        this IRabbitExchangeBuilder<T> builder,
        IRabbitExchangeBuilder exchange)
    {
        return builder.AlternateTo<IRabbitExchangeBuilder<T>>(exchange);
    }
    
    #endregion
}