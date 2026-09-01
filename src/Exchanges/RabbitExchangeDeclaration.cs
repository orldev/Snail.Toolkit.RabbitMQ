using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Exchanges;

/// <summary>
/// Represents the configuration for declaring a RabbitMQ exchange.
/// </summary>
/// <param name="connectionDeclaration">The connection configuration this exchange belongs to.</param>
/// <param name="type">The type of exchange (direct, fanout, topic, headers).</param>
/// <param name="name">The name of the exchange.</param>
public sealed class RabbitExchangeDeclaration(
    RabbitConnectionDeclaration connectionDeclaration,
    string type,
    string name)
{
    /// <summary>
    /// Gets the connection configuration this exchange belongs to.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    /// <summary>
    /// Gets the type of exchange (direct, fanout, topic, headers).
    /// </summary>
    public string Type { get; } = type;

    /// <summary>
    /// Gets the name of the exchange.
    /// </summary>
    public string Name { get; } = name;

    /// <summary>
    /// Gets or sets whether the exchange is durable (survives server restarts).
    /// </summary>
    public bool Durable { get; set; }

    /// <summary>
    /// Gets or sets whether the server should not wait for exchange declaration confirmation.
    /// </summary>
    public bool NoWait { get; set; }

    /// <summary>
    /// Gets or sets whether the exchange should auto-delete when no longer in use.
    /// </summary>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Gets or sets whether this declaration should delete the exchange rather than create it.
    /// </summary>
    public bool Deleted { get; set; }

    /// <summary>
    /// Gets or sets whether the exchange should only be deleted if it's unused (when Deleted is true).
    /// </summary>
    public bool UnusedOnly { get; set; }

    /// <summary>
    /// Gets additional exchange arguments for advanced configuration.
    /// </summary>
    public IDictionary<string, object?> Arguments { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// Gets the list of bindings from this exchange to other exchanges.
    /// </summary>
    public IList<RabbitExchangeBindingDeclaration> BindingDeclarations { get; } = new List<RabbitExchangeBindingDeclaration>();
}