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
    private readonly List<Action<IBasicProperties>> _properties = [];

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
    /// Gets the actions that set the message properties of every publish, in the order they run.
    /// </summary>
    /// <remarks>
    /// Each action modifies the <see cref="IBasicProperties"/> of a published message: headers, delivery mode and
    /// the like. Added through the Property extension of the producer builder and the extensions built on it.
    /// </remarks>
    public IReadOnlyList<Action<IBasicProperties>> Properties => _properties;

    /// <summary>
    /// Creates a new producer declaration based on an existing declaration.
    /// </summary>
    /// <param name="declaration">The source declaration to copy.</param>
    /// <returns>A new <see cref="RabbitProducerDeclaration"/> with the same configuration.</returns>
    /// <remarks>
    /// The copy has a list of <see cref="Properties"/> of its own, so actions an override adds never reach the
    /// original. The list is sized for one more action: a per-publish override usually adds one, and then the list
    /// does not grow again.
    /// </remarks>
    public static RabbitProducerDeclaration FromDeclaration(RabbitProducerDeclaration declaration)
    {
        var copy = new RabbitProducerDeclaration(
            declaration.PayloadType,
            declaration.ConnectionDeclaration,
            declaration.ExchangeDeclaration)
        {
            RoutingKey = declaration.RoutingKey,
            Mandatory = declaration.Mandatory,
            Transactional = declaration.Transactional
        };

        copy._properties.EnsureCapacity(declaration._properties.Count + 1);
        copy._properties.AddRange(declaration._properties);

        return copy;
    }

    /// <summary>
    /// Adds an action that sets message properties, after the ones added before it.
    /// </summary>
    /// <param name="property">The action.</param>
    internal void AddProperty(Action<IBasicProperties> property) => _properties.Add(property);
}
