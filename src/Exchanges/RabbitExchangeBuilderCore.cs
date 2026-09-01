namespace Snail.Toolkit.RabbitMQ.Exchanges;

/// <summary>
/// Represents the core functionality for building RabbitMQ exchange configurations.
/// </summary>
public interface IRabbitExchangeBuilderCore
{
    /// <summary>
    /// Gets the exchange declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitExchangeDeclaration"/> instance containing exchange configuration.</value>
    RabbitExchangeDeclaration ExchangeDeclaration { get; }
}

/// <summary>
/// Represents a builder for configuring RabbitMQ exchanges.
/// </summary>
public interface IRabbitExchangeBuilder : IRabbitExchangeBuilderCore
{
}

/// <summary>
/// Default implementation of <see cref="IRabbitExchangeBuilder"/> for untyped exchange configuration.
/// </summary>
/// <param name="exchangeDeclaration">The exchange declaration to be managed by this builder.</param>
internal sealed class RabbitExchangeBuilder(RabbitExchangeDeclaration exchangeDeclaration)
    : IRabbitExchangeBuilder
{
    /// <inheritdoc/>
    public RabbitExchangeDeclaration ExchangeDeclaration { get; } = exchangeDeclaration;
}

/// <summary>
/// Represents a typed builder for configuring RabbitMQ exchanges with message type association.
/// </summary>
/// <typeparam name="T">The type of message payload associated with this exchange.</typeparam>
public interface IRabbitExchangeBuilder<in T> : IRabbitExchangeBuilderCore
{
}

/// <summary>
/// Default implementation of <see cref="IRabbitExchangeBuilder{T}"/> for typed exchange configuration.
/// </summary>
/// <typeparam name="T">The type of message payload associated with this exchange.</typeparam>
/// <param name="exchangeDeclaration">The exchange declaration to be managed by this builder.</param>
internal sealed class RabbitExchangeBuilder<T>(RabbitExchangeDeclaration exchangeDeclaration)
    : IRabbitExchangeBuilder<T>
{
    /// <inheritdoc/>
    public RabbitExchangeDeclaration ExchangeDeclaration { get; } = exchangeDeclaration;
}