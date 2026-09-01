using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Queues.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IRabbitConnectionBuilder"/> to configure RabbitMQ queues.
/// </summary>
public static partial class RabbitConnectionBuilderExtensions
{
    /// <summary>
    /// Adds a new queue declaration to the RabbitMQ configuration.
    /// </summary>
    /// <typeparam name="T">The type associated with this queue, typically used for message serialization/deserialization.</typeparam>
    /// <param name="connection">The connection builder instance.</param>
    /// <param name="queueName">The name of the queue to declare.</param>
    /// <returns>A <see cref="IRabbitQueueBuilder{T}"/> instance for further queue configuration.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="queueName"/> is null.</exception>
    /// <remarks>
    /// This method registers a queue declaration that will be created when the application starts.
    /// The queue will be associated with the connection specified in the builder.
    /// Use the returned <see cref="IRabbitQueueBuilder{T}"/> to configure additional queue properties.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddRabbitConnection(connection => connection
    ///     .AddQueue&lt;MyMessage&gt;("my-queue")
    ///     .Durable()
    ///     .AutoDelete());
    /// </code>
    /// </example>
    public static IRabbitQueueBuilder<T> AddQueue<T>(
        this IRabbitConnectionBuilder connection,
        string queueName)
    {
        ArgumentNullException.ThrowIfNull(queueName);
        
        var declaration = new RabbitQueueDeclaration(connection.ConnectionDeclaration, queueName);

        connection.Services.Configure<RabbitOptions>(options => 
            options.QueueDeclarations.Add(declaration));

        return new RabbitQueueBuilder<T>(declaration);
    }
}