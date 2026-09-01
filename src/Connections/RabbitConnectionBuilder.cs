using Microsoft.Extensions.DependencyInjection;

namespace Snail.Toolkit.RabbitMQ.Connections;

/// <summary>
/// Represents a builder for configuring RabbitMQ connections and their associated services.
/// </summary>
public interface IRabbitConnectionBuilder
{
    /// <summary>
    /// Gets the service collection where RabbitMQ services will be registered.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Gets the connection declaration containing the configuration for this RabbitMQ connection.
    /// </summary>
    RabbitConnectionDeclaration ConnectionDeclaration { get; }
}

/// <summary>
/// Default implementation of <see cref="IRabbitConnectionBuilder"/> that manages RabbitMQ connection configuration.
/// </summary>
/// <param name="services">The service collection for dependency injection registrations.</param>
/// <param name="connectionDeclaration">The configuration declaration for the RabbitMQ connection.</param>
internal sealed class RabbitConnectionBuilder(
    IServiceCollection services,
    RabbitConnectionDeclaration connectionDeclaration)
    : IRabbitConnectionBuilder
{
    /// <inheritdoc/>
    public IServiceCollection Services { get; } = services;

    /// <inheritdoc/>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;
}