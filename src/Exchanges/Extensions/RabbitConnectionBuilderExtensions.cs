using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Exchanges.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitConnectionBuilder"/> to configure RabbitMQ exchanges.
/// </summary>
public static partial class RabbitConnectionBuilderExtensions
{
    /// <summary>
    /// Adds a new exchange declaration to the RabbitMQ configuration.
    /// </summary>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="type">The type of exchange (direct, fanout, topic, headers).</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder"/> for further exchange configuration.</returns>
    public static IRabbitExchangeBuilder AddExchange(
        this IRabbitConnectionBuilder connection,
        string type,
        string name)
    {
        var declaration = new RabbitExchangeDeclaration(connection.ConnectionDeclaration, type, name);

        connection.Services
            .Configure<RabbitOptions>(options => 
                options.ExchangeDeclarations.Add(declaration));

        return new RabbitExchangeBuilder(declaration);
    }

    /// <summary>
    /// Adds a new typed exchange declaration to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this exchange.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="type">The type of exchange (direct, fanout, topic, headers).</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder{T}"/> for further exchange configuration.</returns>
    public static IRabbitExchangeBuilder<T> AddExchange<T>(
        this IRabbitConnectionBuilder connection,
        string type,
        string name)
    {
        var declaration = new RabbitExchangeDeclaration(connection.ConnectionDeclaration, type, name);

        connection.Services
            .Configure<RabbitOptions>(options => options.ExchangeDeclarations.Add(declaration));

        return new RabbitExchangeBuilder<T>(declaration);
    }

    /// <summary>
    /// Adds a new direct exchange to the RabbitMQ configuration.
    /// </summary>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Direct exchanges route messages to queues based on exact routing key matches.
    /// </remarks>
    public static IRabbitExchangeBuilder AddDirectExchange(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange(ExchangeType.Direct, name);
    }

    /// <summary>
    /// Adds a new typed direct exchange to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this exchange.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder{T}"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Direct exchanges route messages to queues based on exact routing key matches.
    /// </remarks>
    public static IRabbitExchangeBuilder<T> AddDirectExchange<T>(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange<T>(ExchangeType.Direct, name);
    }

    /// <summary>
    /// Adds a new typed fanout exchange to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this exchange.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder{T}"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Fanout exchanges route messages to all bound queues unconditionally.
    /// </remarks>
    public static IRabbitExchangeBuilder<T> AddFanoutExchange<T>(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange<T>(ExchangeType.Fanout, name);
    }

    /// <summary>
    /// Adds a new topic exchange to the RabbitMQ configuration.
    /// </summary>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Topic exchanges route messages based on wildcard pattern matching of routing keys.
    /// </remarks>
    public static IRabbitExchangeBuilder AddTopicExchange(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange(ExchangeType.Topic, name);
    }

    /// <summary>
    /// Adds a new typed topic exchange to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this exchange.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder{T}"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Topic exchanges route messages based on wildcard pattern matching of routing keys.
    /// </remarks>
    public static IRabbitExchangeBuilder<T> AddTopicExchange<T>(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange<T>(ExchangeType.Topic, name);
    }

    /// <summary>
    /// Adds a new headers exchange to the RabbitMQ configuration.
    /// </summary>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Headers exchanges route messages based on header values rather than routing keys.
    /// </remarks>
    public static IRabbitExchangeBuilder AddHeadersExchange(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange(ExchangeType.Headers, name);
    }

    /// <summary>
    /// Adds a new typed headers exchange to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this exchange.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="name">The name of the exchange.</param>
    /// <returns>An <see cref="IRabbitExchangeBuilder{T}"/> for further exchange configuration.</returns>
    /// <remarks>
    /// Headers exchanges route messages based on header values rather than routing keys.
    /// </remarks>
    public static IRabbitExchangeBuilder<T> AddHeadersExchange<T>(
        this IRabbitConnectionBuilder connection,
        string name)
    {
        return connection.AddExchange<T>(ExchangeType.Headers, name);
    }
}