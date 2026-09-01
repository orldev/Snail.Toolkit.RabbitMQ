namespace Snail.Toolkit.RabbitMQ.Exchanges;

/// <summary>
/// Represents a builder for configuring bindings between RabbitMQ exchanges.
/// </summary>
public interface IRabbitExchangeBindingBuilder
{
    /// <summary>
    /// Gets the exchange binding declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitExchangeBindingDeclaration"/> instance containing the binding configuration.</value>
    RabbitExchangeBindingDeclaration Declaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitExchangeBindingBuilder"/> that holds an exchange binding declaration.
/// </summary>
/// <param name="declaration">The exchange binding declaration to be managed by this builder.</param>
/// <remarks>
/// This class provides a concrete implementation of the exchange binding builder interface,
/// serving as a container for the binding configuration between exchanges.
/// </remarks>
internal sealed class RabbitExchangeBindingBuilder(RabbitExchangeBindingDeclaration declaration)
    : IRabbitExchangeBindingBuilder
{
    /// <inheritdoc/>
    public RabbitExchangeBindingDeclaration Declaration { get; } = declaration;
}