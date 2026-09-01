using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Consumers.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitConnectionBuilder"/> to configure RabbitMQ consumers.
/// </summary>
public static partial class RabbitConnectionBuilderExtensions
{
    /// <summary>
    /// Adds a message consumer for the specified message type to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type of message payload this consumer will handle.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="queues">One or more queue builders from which the consumer will receive messages.</param>
    /// <returns>A <see cref="IRabbitConsumerBuilder{T}"/> instance for further consumer configuration.</returns>
    /// <exception cref="ArgumentNullException">Thrown if no queues are provided.</exception>
    /// <remarks>
    /// This method:
    /// 1. Creates a new consumer declaration for the specified message type
    /// 2. Associates the consumer with the provided queues
    /// 3. Registers the consumer in the DI container
    /// 4. Returns a builder for further consumer configuration
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddRabbitConnection(connection => connection
    ///     .AddConsumer&lt;MyMessage&gt;(queueBuilder)
    ///     .Prefetch(10)
    ///     .Subscribe(HandleMessageAsync));
    /// </code>
    /// </example>
    public static IRabbitConsumerBuilder<T> AddConsumer<T>(
        this IRabbitConnectionBuilder connection,
        params IRabbitQueueBuilder<T>[] queues)
    {
        var declaration = new RabbitConsumerDeclaration(
            typeof(T),
            connection.ConnectionDeclaration,
            queues.Select(queue => queue.Declaration).ToArray());

        connection.Services
            .Configure<RabbitOptions>(
                options => options.ConsumerDeclarations.Add(declaration));

        return new RabbitConsumerBuilder<T>(connection.Services, declaration);
    }
}