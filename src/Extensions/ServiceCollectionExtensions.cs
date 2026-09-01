using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Exchanges;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IServiceCollection"/> to configure RabbitMQ services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures core RabbitMQ services and options.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="options">The action to configure RabbitMQ options.</param>
    /// <returns>An <see cref="IRabbitBuilder"/> for further configuration.</returns>
    /// <remarks>
    /// This internal method registers all core RabbitMQ services including:
    /// - Configuration options
    /// - Producer, channel and connection providers
    /// - Hosted services for exchanges, queues and consumers
    /// </remarks>
    private static IRabbitBuilder AddOptions(
        this IServiceCollection services,
        Action<RabbitOptions> options)
    {
        services.Configure(options)
            .AddSingleton<IRabbitProducer, RabbitProducer>()
            .AddSingleton<IRabbitChannelProvider, RabbitChannelProvider>()
            .AddSingleton<IRabbitConnectionProvider, RabbitConnectionProvider>()
            .AddHostedService<RabbitExchangeHostedService>()
            .AddHostedService<RabbitQueueHostedService>()
            .AddHostedService<RabbitConsumerHostedService>();

        return new RabbitBuilder(services);
    }
    
    /// <summary>
    /// Adds and configures RabbitMQ services to the service collection.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="options">The action to configure RabbitMQ options.</param>
    /// <param name="connections">Optional action to configure RabbitMQ connections.</param>
    /// <returns>The service collection for method chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddRabbit(options => 
    /// {
    ///     options.UseConnectionFactory(factory => 
    ///     {
    ///         factory.HostName = "localhost";
    ///         factory.UserName = "guest";
    ///         factory.Password = "guest";
    ///     });
    /// },
    /// connections => 
    /// {
    ///     connections.AddConnection("orders");
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddRabbit(
        this IServiceCollection services,
        Action<RabbitOptions> options,
        Action<IRabbitBuilder>? connections = null)
    {
        var builder = services.AddOptions(options);
        connections?.Invoke(builder);
        return builder.Services;
    }
    
    /// <summary>
    /// Adds and configures RabbitMQ services using configuration from <see cref="IConfiguration"/>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configuration">The configuration containing RabbitMQ settings.</param>
    /// <param name="connections">The action to configure RabbitMQ connections.</param>
    /// <param name="name">The name of the configuration section (default: "RabbitMQ").</param>
    /// <returns>The service collection for method chaining.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="configuration"/> is null or the configuration section is not found.
    /// </exception>
    /// <remarks>
    /// The configuration section should match the <see cref="RabbitClientOptions"/> structure.
    /// </remarks>
    public static IServiceCollection AddRabbit(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<IRabbitBuilder> connections,
        string name = "RabbitMQ")
    {
        ArgumentNullException.ThrowIfNull(configuration);
        
        var options = configuration
            .GetSection(name)
            .Get<RabbitClientOptions>();
        
        ArgumentNullException.ThrowIfNull(options);
        
        var builder = services.AddOptions(o => 
            o.UseConnectionFactory(client =>
            {
                client.HostName = options.HostName;
                client.UserName = options.UserName;
                client.Password = options.Password;
                client.ClientProvidedName = options.ClientProvidedName;
            }));
        
        connections.Invoke(builder);
        
        return builder.Services;
    }
}