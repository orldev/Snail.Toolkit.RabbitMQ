using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Queues;

/// <summary>
/// Represents the declaration and configuration of a RabbitMQ queue.
/// </summary>
/// <param name="connectionDeclaration">The connection declaration used to connect to the RabbitMQ server.</param>
/// <param name="name">The name of the queue.</param>
public sealed class RabbitQueueDeclaration(
    RabbitConnectionDeclaration connectionDeclaration,
    string name)
{
    /// <summary>
    /// Gets the connection declaration used to connect to the RabbitMQ server.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    /// <summary>
    /// Gets the name of the queue.
    /// </summary>
    public string Name { get; } = name;

    /// <summary>
    /// Gets or sets a value indicating whether the queue survives server restarts.
    /// </summary>
    /// <value>true if the queue should be durable; otherwise, false.</value>
    public bool Durable { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue should be exclusive to this connection.
    /// </summary>
    /// <value>true if the queue should be exclusive; otherwise, false.</value>
    /// <remarks>Exclusive queues are deleted when the connection closes.</remarks>
    public bool Exclusive { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue should be automatically deleted when no longer in use.
    /// </summary>
    /// <value>true if the queue should auto-delete; otherwise, false.</value>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the server should respond without waiting for queue creation.
    /// </summary>
    /// <value>true to not wait for confirmation; otherwise, false.</value>
    public bool NoWait { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue should be deleted rather than declared.
    /// </summary>
    /// <value>true if the queue should be deleted; false if it should be created.</value>
    public bool Deleted { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue should only be deleted if it's unused.
    /// </summary>
    /// <value>true to only delete if unused; otherwise, false.</value>
    /// <remarks>Only applies when <see cref="Deleted"/> is true.</remarks>
    public bool UnusedOnly { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the queue should only be deleted if it's empty.
    /// </summary>
    /// <value>true to only delete if empty; otherwise, false.</value>
    /// <remarks>Only applies when <see cref="Deleted"/> is true.</remarks>
    public bool EmptyOnly { get; set; }

    /// <summary>
    /// Gets the companion retry queue declaration, if retry with backoff is configured.
    /// </summary>
    /// <value>
    /// A queue named "{Name}.retry" that holds rejected messages for the configured delay
    /// and dead-letters them back into this queue, or null when retry is not configured.
    /// </value>
    /// <remarks>Configured via the WithRetry queue builder extension; not intended to be set directly.</remarks>
    public RabbitQueueDeclaration? RetryQueue { get; internal set; }

    /// <summary>
    /// Gets the maximum number of processing attempts per message before it is dropped.
    /// </summary>
    /// <value>Meaningful only when <see cref="RetryQueue"/> is configured.</value>
    public int MaxAttempts { get; internal set; }

    /// <summary>
    /// Gets additional queue arguments that can be used to configure advanced queue features.
    /// </summary>
    /// <value>A dictionary of queue arguments where the key is the argument name.</value>
    /// <remarks>
    /// These arguments are passed to RabbitMQ when creating the queue and can be used to configure
    /// features like queue length limits, dead letter exchanges, etc.
    /// </remarks>
    public IDictionary<string, object?> Arguments { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// Gets the collection of binding declarations that define how this queue is bound to exchanges.
    /// </summary>
    /// <value>A list of queue binding declarations.</value>
    public IList<RabbitQueueBindingDeclaration> BindingDeclarations { get; } = new List<RabbitQueueBindingDeclaration>();
}