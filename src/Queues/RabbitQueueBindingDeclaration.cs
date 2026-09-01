using Snail.Toolkit.RabbitMQ.Exchanges;

namespace Snail.Toolkit.RabbitMQ.Queues;

/// <summary>
/// Represents the declaration of a binding between a RabbitMQ queue and exchange.
/// </summary>
/// <param name="exchangeDeclaration">The exchange declaration that this binding connects to.</param>
public sealed class RabbitQueueBindingDeclaration(
    RabbitExchangeDeclaration exchangeDeclaration)
{
    /// <summary>
    /// Gets the exchange declaration that this binding connects to.
    /// </summary>
    /// <value>The <see cref="RabbitExchangeDeclaration"/> instance representing the target exchange.</value>
    public RabbitExchangeDeclaration ExchangeDeclaration { get; } = exchangeDeclaration;

    /// <summary>
    /// Gets or sets a value indicating whether the server should respond without waiting for the binding operation to complete.
    /// </summary>
    /// <value>true to not wait for confirmation; otherwise, false.</value>
    public bool NoWait { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this binding should be removed rather than created.
    /// </summary>
    /// <value>true if the binding should be deleted; false if it should be created.</value>
    public bool Deleted { get; set; }

    /// <summary>
    /// Gets or sets the routing key used for this binding.
    /// </summary>
    /// <value>The routing key that determines which messages are routed from the exchange to the queue.</value>
    /// <remarks>
    /// If not specified, the default routing behavior of the exchange type will be used.
    /// </remarks>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Gets additional binding arguments that can be used to configure advanced binding features.
    /// </summary>
    /// <value>A dictionary of binding arguments where the key is the argument name.</value>
    /// <remarks>
    /// These arguments are passed to RabbitMQ when creating the binding and can be used to configure
    /// features like header matching, priority handling, etc.
    /// </remarks>
    public IDictionary<string, object?> Arguments { get; } = new Dictionary<string, object?>();
}