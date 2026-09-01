namespace Snail.Toolkit.RabbitMQ.Producers;

/// <summary>
/// Represents a builder for configuring RabbitMQ message producers for a specific message type.
/// </summary>
/// <typeparam name="T">The type of message payload this producer will handle.</typeparam>
/// <remarks>
/// This interface provides access to the underlying producer declaration for configuration
/// and is typically used with extension methods to provide a fluent configuration API.
/// </remarks>
public interface IRabbitProducerBuilder<T>
{
    /// <summary>
    /// Gets the RabbitMQ producer declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitProducerDeclaration"/> instance containing the producer configuration.</value>
    RabbitProducerDeclaration ProducerDeclaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitProducerBuilder{T}"/> that holds a RabbitMQ producer declaration.
/// </summary>
/// <typeparam name="T">The type of message payload this producer will handle.</typeparam>
/// <param name="producerDeclaration">The producer declaration to be managed by this builder.</param>
/// <remarks>
/// This class provides a concrete implementation of the producer builder interface,
/// serving as a container for the producer configuration with type association.
/// </remarks>
internal sealed class RabbitProducerBuilder<T>(RabbitProducerDeclaration producerDeclaration)
    : IRabbitProducerBuilder<T>
{
    /// <inheritdoc/>
    public RabbitProducerDeclaration ProducerDeclaration { get; } = producerDeclaration;
}