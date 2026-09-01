using Microsoft.Extensions.DependencyInjection;

namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Defines an interface for building and configuring RabbitMQ services.
/// </summary>
public interface IRabbitBuilder
{
    /// <summary>
    /// Gets the collection of services for the application.
    /// </summary>
    IServiceCollection Services { get; }
}

/// <summary>
/// An internal sealed implementation of <see cref="IRabbitBuilder"/> that provides access to the service collection.
/// </summary>
/// <param name="services">The <see cref="IServiceCollection"/> to be used for configuring services.</param>
internal sealed class RabbitBuilder(IServiceCollection services) : IRabbitBuilder
{
    /// <summary>
    /// Gets the collection of services for the application.
    /// </summary>
    public IServiceCollection Services { get; } = services;
}