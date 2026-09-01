using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Exchanges;

namespace Snail.Toolkit.RabbitMQ.Producers.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitConnectionBuilder"/> to configure RabbitMQ message producers.
/// </summary>
public static partial class RabbitConnectionBuilderExtensions
{
    /// <summary>
    /// Adds a message producer for the specified message type to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type of message payload this producer will handle.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="exchange">Optional exchange builder where messages will be published.</param>
    /// <returns>A <see cref="IRabbitProducerBuilder{T}"/> instance for further producer configuration.</returns>
    /// <remarks>
    /// If no exchange is specified, messages will be published to the default exchange (empty string).
    /// The producer will be registered in the DI container and can be resolved via <see cref="IRabbitProducer"/>.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddRabbitConnection(connection => connection
    ///     .AddProducer&lt;MyMessage&gt;(exchange)
    ///     .RoutedTo("my.routing.key")
    ///     .Persistent());
    /// </code>
    /// </example>
    public static IRabbitProducerBuilder<T> AddProducer<T>(
        this IRabbitConnectionBuilder connection,
        IRabbitExchangeBuilderCore? exchange = null)
    {
        var declaration = new RabbitProducerDeclaration(
            typeof(T),
            connection.ConnectionDeclaration,
            exchange?.ExchangeDeclaration);

        connection.Services.Configure<RabbitOptions>(options => 
            options.ProducerDeclarations.Add(typeof(T), declaration));

        return new RabbitProducerBuilder<T>(declaration);
    }

    /// <summary>
    /// Adds a message producer for the specified message type bound to an exchange.
    /// </summary>
    /// <typeparam name="T">The type of message payload this producer will handle.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="exchange">The exchange builder where messages will be published.</param>
    /// <returns>A <see cref="IRabbitProducerBuilder{T}"/> instance for further producer configuration.</returns>
    /// <remarks>
    /// This overload provides a strongly-typed way to associate a producer with a specific exchange.
    /// </remarks>
    public static IRabbitProducerBuilder<T> AddProducer<T>(
        this IRabbitConnectionBuilder connection,
        IRabbitExchangeBuilder exchange)
    {
        return connection.AddProducer<T>(exchange: (IRabbitExchangeBuilderCore)exchange);
    }
        
    /// <summary>
    /// Adds a message producer for the specified message type bound to a typed exchange.
    /// </summary>
    /// <typeparam name="T">The type of message payload this producer will handle.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="exchange">The typed exchange builder where messages will be published.</param>
    /// <returns>A <see cref="IRabbitProducerBuilder{T}"/> instance for further producer configuration.</returns>
    /// <remarks>
    /// This overload provides a strongly-typed way to associate a producer with a specific exchange
    /// that shares the same message type.
    /// </remarks>
    public static IRabbitProducerBuilder<T> AddProducer<T>(
        this IRabbitConnectionBuilder connection,
        IRabbitExchangeBuilder<T> exchange)
    {
        return connection.AddProducer<T>(exchange: (IRabbitExchangeBuilderCore)exchange);
    }
}