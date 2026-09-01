using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitBuilder"/> to configure RabbitMQ connections.
/// </summary>
public static class RabbitBuilderExtensions
{
    /// <summary>
    /// Adds a named RabbitMQ connection to the configuration.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder instance.</param>
    /// <param name="name">The name of the connection configuration.</param>
    /// <param name="connection">The action to configure the connection.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// This method:
    /// 1. Creates a new connection declaration with the specified name
    /// 2. Adds it to the RabbitMQ options
    /// 3. Invokes the configuration action with a new connection builder
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.AddConnection("orders", connection => 
    ///     connection.UseConnectionUrl("amqp://user:pass@host:port/vhost")
    ///     .UseClientProvidedName("orders-service"));
    /// </code>
    /// </example>
    public static IRabbitBuilder AddConnection(
        this IRabbitBuilder builder,
        string name,
        Action<IRabbitConnectionBuilder> connection)
    {
        var declaration = new RabbitConnectionDeclaration(name);

        builder.Services.Configure<RabbitOptions>(options => 
            options.ConnectionDeclarations.Add(declaration));

        connection.Invoke(new RabbitConnectionBuilder(builder.Services, declaration));

        return builder;
    }

    /// <summary>
    /// Adds a default RabbitMQ connection to the configuration.
    /// </summary>
    /// <param name="builder">The RabbitMQ builder instance.</param>
    /// <param name="connection">The action to configure the connection.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <remarks>
    /// This is a convenience method that creates a connection named "default".
    /// </remarks>
    public static IRabbitBuilder AddConnection(
        this IRabbitBuilder builder,
        Action<IRabbitConnectionBuilder> connection)
    {
        return builder.AddConnection("default", connection);
    }
}