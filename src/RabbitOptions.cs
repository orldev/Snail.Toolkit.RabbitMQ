using System.Text.Json;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Exchanges;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Provides options for configuring RabbitMQ connections, serialization, and declarations.
/// </summary>
public sealed class RabbitOptions
{
    /// <summary>
    /// Gets the connection factory used to create RabbitMQ connections.
    /// </summary>
    internal ConnectionFactory ConnectionFactory { get; } = new();

    /// <summary>
    /// Gets or sets the function used to serialize objects into byte arrays.
    /// The default implementation uses <see cref="JsonSerializer.SerializeToUtf8Bytes(object, System.Type, JsonSerializerOptions?)"/>.
    /// </summary>
    internal Func<object, byte[]> Serializer { get; set; } = 
        payload => JsonSerializer.SerializeToUtf8Bytes(payload);
    
    /// <summary>
    /// Gets or sets the function used to deserialize byte arrays into objects of a specified type.
    /// The default implementation uses <see cref="JsonSerializer"/>.
    /// </summary>
    internal Func<byte[], Type, object?> Deserializer { get; set; } =
        (bytes, type) => JsonSerializer.Deserialize(bytes, type);
    
    /// <summary>
    /// Gets a list of connection declarations.
    /// </summary>
    internal IList<RabbitConnectionDeclaration> ConnectionDeclarations { get; } = new List<RabbitConnectionDeclaration>();
    
    /// <summary>
    /// Gets a list of exchange declarations.
    /// </summary>
    internal IList<RabbitExchangeDeclaration> ExchangeDeclarations { get; } = new List<RabbitExchangeDeclaration>();
    
    /// <summary>
    /// Gets a list of queue declarations.
    /// </summary>
    internal IList<RabbitQueueDeclaration> QueueDeclarations { get; } = new List<RabbitQueueDeclaration>();
    
    /// <summary>
    /// Gets a list of consumer declarations.
    /// </summary>
    internal IList<RabbitConsumerDeclaration> ConsumerDeclarations { get; } = new List<RabbitConsumerDeclaration>();
    
    /// <summary>
    /// Gets a dictionary of producer declarations, keyed by the type of the produced message.
    /// </summary>
    internal IDictionary<Type, RabbitProducerDeclaration> ProducerDeclarations { get; } = new Dictionary<Type, RabbitProducerDeclaration>();
}