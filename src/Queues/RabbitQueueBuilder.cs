namespace Snail.Toolkit.RabbitMQ.Queues;

/// <summary>
/// Represents a builder for configuring RabbitMQ queue declarations for a specific type.
/// </summary>
/// <typeparam name="T">The type associated with this queue configuration.</typeparam>
public interface IRabbitQueueBuilder<T>
{
    /// <summary>
    /// Gets the RabbitMQ queue declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitQueueDeclaration"/> instance that contains the queue configuration.</value>
    RabbitQueueDeclaration Declaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitQueueBuilder{T}"/> that holds a RabbitMQ queue declaration.
/// </summary>
/// <typeparam name="T">The type associated with this queue configuration.</typeparam>
/// <param name="declaration">The RabbitMQ queue declaration to be managed by this builder.</param>
/// <remarks>
/// This class provides a concrete implementation of the queue builder interface,
/// serving as a container for the queue declaration with type association.
/// </remarks>
internal sealed class RabbitQueueBuilder<T>(RabbitQueueDeclaration declaration)
    : IRabbitQueueBuilder<T>
{
    /// <summary>
    /// Gets the RabbitMQ queue declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitQueueDeclaration"/> instance that contains the queue configuration.</value>
    public RabbitQueueDeclaration Declaration { get; } = declaration;
}