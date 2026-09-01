using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// Represents the configuration for a RabbitMQ message consumer.
/// </summary>
/// <param name="payloadType">The type of message payload this consumer will handle.</param>
/// <param name="connectionDeclaration">The connection declaration used to connect to RabbitMQ.</param>
/// <param name="queueDeclarations">The queue declarations from which this consumer will receive messages.</param>
/// <remarks>
/// This class encapsulates all configuration needed to consume messages from RabbitMQ,
/// including connection details, queue subscriptions, and message handling behavior.
/// </remarks>
public sealed class RabbitConsumerDeclaration(
    Type payloadType,
    RabbitConnectionDeclaration connectionDeclaration,
    RabbitQueueDeclaration[] queueDeclarations)
{
    /// <summary>
    /// Gets the type of message payload this consumer will handle.
    /// </summary>
    public Type PayloadType { get; } = payloadType;

    /// <summary>
    /// Gets the connection declaration used to connect to RabbitMQ.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    /// <summary>
    /// Gets the queue declarations from which this consumer will receive messages.
    /// </summary>
    public RabbitQueueDeclaration[] QueueDeclarations { get; } = queueDeclarations;

    /// <summary>
    /// Gets the list of subscription handlers that will process incoming messages.
    /// </summary>
    public IList<Func<IServiceScope, object, CancellationToken, ValueTask>> Subscriptions { get; } = 
        new List<Func<IServiceScope, object, CancellationToken, ValueTask>>();

    /// <summary>
    /// Gets or sets the consumer tag for identifying this consumer.
    /// </summary>
    public string? Tag { get; set; }

    /// <summary>
    /// Gets or sets the prefetch count (QoS) limiting unacknowledged messages.
    /// </summary>
    public ushort PrefetchCount { get; set; }

    /// <summary>
    /// Gets or sets whether the prefetch limit is applied globally across all consumers on the channel.
    /// </summary>
    public bool Global { get; set; }

    /// <summary>
    /// Gets or sets the number of parallel consumers to create for this configuration.
    /// </summary>
    public uint Count { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether this is an exclusive consumer (no other consumers allowed on the queue).
    /// </summary>
    public bool Exclusive { get; set; }

    /// <summary>
    /// Gets or sets whether the broker should not deliver messages published on the same connection.
    /// </summary>
    public bool NoLocal { get; set; }

    /// <summary>
    /// Gets or sets whether messages should be automatically acknowledged upon receipt.
    /// </summary>
    public bool AutoAck { get; set; }

    /// <summary>
    /// Gets or sets whether failed messages should be requeued.
    /// </summary>
    public bool Requeue { get; set; }

    /// <summary>
    /// Gets or sets whether to negatively acknowledge multiple messages when requeuing.
    /// </summary>
    public bool Multiple { get; set; }

    /// <summary>
    /// Gets additional arguments that can be used when consuming messages.
    /// </summary>
    public IDictionary<string, object?> Arguments { get; } = new Dictionary<string, object?>();
}