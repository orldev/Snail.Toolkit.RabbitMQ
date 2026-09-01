using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Exchanges;

namespace Snail.Toolkit.RabbitMQ.Producers;

/// <summary>
/// Represents the configuration for a RabbitMQ message producer.
/// </summary>
/// <param name="payloadType">The type of message payload this producer will handle.</param>
/// <param name="connectionDeclaration">The connection declaration used to connect to RabbitMQ.</param>
/// <param name="exchangeDeclaration">Optional exchange declaration where messages will be published.</param>
/// <remarks>
/// This class encapsulates all configuration needed to publish messages to RabbitMQ,
/// including connection details, exchange/routing information, and message properties.
/// </remarks>
public sealed class RabbitProducerDeclaration(
    Type payloadType,
    RabbitConnectionDeclaration connectionDeclaration,
    RabbitExchangeDeclaration? exchangeDeclaration = null)
{
    /// <summary>
    /// Gets the type of message payload this producer will handle.
    /// </summary>
    public Type PayloadType { get; } = payloadType;

    /// <summary>
    /// Gets the connection declaration used to connect to RabbitMQ.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    /// <summary>
    /// Gets the optional exchange declaration where messages will be published.
    /// </summary>
    /// <remarks>
    /// If null, messages will be published to the default exchange.
    /// </remarks>
    public RabbitExchangeDeclaration? ExchangeDeclaration { get; } = exchangeDeclaration;

    /// <summary>
    /// Gets or sets the routing key used when publishing messages.
    /// </summary>
    /// <remarks>
    /// The routing key determines how messages are routed from the exchange to queues.
    /// For direct exchanges, this typically matches queue names.
    /// </remarks>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether messages must be routed to at least one queue.
    /// </summary>
    /// <value>
    /// true if the server should return an unroutable message with a Return method;
    /// false if the server should silently drop the message.
    /// </value>
    public bool Mandatory { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether publishing should be transactional.
    /// </summary>
    /// <value>
    /// true to use transactions for message publishing;
    /// false for non-transactional publishing.
    /// </value>
    public bool Transactional { get; set; }

    /// <summary>
    /// Gets or sets additional arguments that can be used when publishing messages.
    /// </summary>
    /// <remarks>
    /// These arguments may be used by RabbitMQ plugins or for custom routing logic.
    /// </remarks>
    public IDictionary<string, object> Arguments { get; set; } = new Dictionary<string, object>();

    /// <summary>
    /// Gets or sets a list of actions that configure message properties.
    /// </summary>
    /// <remarks>
    /// Each action in this list will be invoked to modify the <see cref="IBasicProperties"/>
    /// of published messages, allowing customization of headers, delivery mode, etc.
    /// </remarks>
    public IList<Action<IBasicProperties>> Properties { get; set; } = new List<Action<IBasicProperties>>();

    /// <summary>
    /// Creates a new producer declaration based on an existing declaration.
    /// </summary>
    /// <param name="declaration">The source declaration to copy.</param>
    /// <returns>A new <see cref="RabbitProducerDeclaration"/> with the same configuration.</returns>
    /// <remarks>
    /// This method performs a deep copy of all mutable properties (Arguments and Properties)
    /// to ensure modifications to the new declaration don't affect the original.
    /// </remarks>
    public static RabbitProducerDeclaration FromDeclaration(RabbitProducerDeclaration declaration)
    {
        return new RabbitProducerDeclaration(
            declaration.PayloadType,
            declaration.ConnectionDeclaration,
            declaration.ExchangeDeclaration)
        {
            RoutingKey = declaration.RoutingKey,
            Mandatory = declaration.Mandatory,
            Transactional = declaration.Transactional,
            Arguments = new Dictionary<string, object>(declaration.Arguments),
            Properties = new List<Action<IBasicProperties>>(declaration.Properties)
        };
    }
}