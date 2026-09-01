using Microsoft.Extensions.DependencyInjection;

namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// Defines a contract for message consumers that handle messages of a specific type.
/// </summary>
/// <typeparam name="T">The type of message this consumer can handle.</typeparam>
public interface IConsumer<in T>
{
    // TODO: fix ValueTask on Task
    /// <summary>
    /// Handles an incoming message asynchronously.
    /// </summary>
    /// <param name="message">The message payload to process.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A ValueTask that represents the asynchronous operation.</returns>
    ValueTask HandleAsync(T message, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a builder for configuring RabbitMQ message consumers for a specific message type.
/// </summary>
/// <typeparam name="T">The type of message payload this consumer will handle.</typeparam>
public interface IRabbitConsumerBuilder<T>
{
    /// <summary>
    /// Gets the service collection for dependency injection registrations.
    /// </summary>
    IServiceCollection Services { get; }
    
    /// <summary>
    /// Gets the RabbitMQ consumer declaration being configured.
    /// </summary>
    /// <value>The <see cref="RabbitConsumerDeclaration"/> instance containing the consumer configuration.</value>
    RabbitConsumerDeclaration ConsumerDeclaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitConsumerBuilder{T}"/> that holds a RabbitMQ consumer declaration.
/// </summary>
/// <typeparam name="T">The type of message payload this consumer will handle.</typeparam>
/// <param name="services">The service collection for dependency injection.</param>
/// <param name="consumerDeclaration">The consumer declaration to be managed by this builder.</param>
/// <remarks>
/// This class provides a concrete implementation of the consumer builder interface,
/// serving as a container for the consumer configuration with type association.
/// It maintains both the service collection for DI registrations and the consumer configuration.
/// </remarks>
internal sealed class RabbitConsumerBuilder<T>(
    IServiceCollection services,
    RabbitConsumerDeclaration consumerDeclaration)
    : IRabbitConsumerBuilder<T>
{
    /// <inheritdoc/>
    public IServiceCollection Services { get; } = services;
    
    /// <inheritdoc/>
    public RabbitConsumerDeclaration ConsumerDeclaration { get; } = consumerDeclaration;
}