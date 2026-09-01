namespace Snail.Toolkit.RabbitMQ.Queues;

/// <summary>
/// Represents a builder for configuring bindings between a RabbitMQ queue and exchange.
/// </summary>
public interface IRabbitQueueBindingBuilder
{
    /// <summary>
    /// Gets the binding declaration being configured by this builder.
    /// </summary>
    /// <value>The <see cref="RabbitQueueBindingDeclaration"/> instance containing binding configuration.</value>
    RabbitQueueBindingDeclaration Declaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitQueueBindingBuilder"/> that holds a queue binding declaration.
/// </summary>
/// <param name="declaration">The binding declaration to be managed by this builder.</param>
/// <remarks>
/// This class provides a concrete implementation for building queue-to-exchange bindings,
/// serving as a container for the binding configuration with fluent API support.
/// </remarks>
internal sealed class RabbitQueueBindingBuilder(RabbitQueueBindingDeclaration declaration)
    : IRabbitQueueBindingBuilder
{
    /// <inheritdoc/>
    public RabbitQueueBindingDeclaration Declaration { get; } = declaration;
}