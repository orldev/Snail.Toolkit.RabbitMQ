namespace Snail.Toolkit.RabbitMQ.Exchanges;

/// <summary>
/// Represents the configuration for a binding between RabbitMQ exchanges.
/// </summary>
/// <param name="exchangeDeclaration">The target exchange declaration for this binding.</param>
public sealed class RabbitExchangeBindingDeclaration(RabbitExchangeDeclaration exchangeDeclaration)
{
    /// <summary>
    /// Gets the target exchange declaration that this binding connects to.
    /// </summary>
    /// <value>The <see cref="RabbitExchangeDeclaration"/> instance representing the target exchange.</value>
    public RabbitExchangeDeclaration ExchangeDeclaration { get; } = exchangeDeclaration;

    /// <summary>
    /// Gets or sets the routing key used for this binding.
    /// </summary>
    /// <value>
    /// The routing key that determines how messages are routed between exchanges.
    /// For fanout exchanges, this value is typically ignored.
    /// </value>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the server should respond without waiting for the binding operation to complete.
    /// </summary>
    /// <value>
    /// true if the server should not wait for confirmation; otherwise, false.
    /// When true, improves performance but reduces reliability.
    /// </value>
    public bool NoWait { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this binding should be removed rather than created.
    /// </summary>
    /// <value>
    /// true if the binding should be deleted; false if it should be created.
    /// </value>
    public bool Deleted { get; set; }

    /// <summary>
    /// Gets additional binding arguments that can be used to configure advanced binding features.
    /// </summary>
    /// <value>
    /// A dictionary of binding arguments where the key is the argument name.
    /// These arguments may be used by RabbitMQ plugins or for custom routing logic.
    /// </value>
    public IDictionary<string, object?> Arguments { get; } = new Dictionary<string, object?>();
}