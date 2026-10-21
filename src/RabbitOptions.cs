using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
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
    /// The options of the default serializer: the serializer's defaults, read by reflection where reflection is allowed.
    /// </summary>
    /// <remarks>
    /// A native application has no reflection-based JSON: there the resolver stays unset and the application passes
    /// its own source-generated serialization through UseSerialization.
    /// </remarks>
    private static readonly JsonSerializerOptions Json = Reflective();

    /// <summary>
    /// The serializer every options instance starts with: the payload's runtime type with the serializer's defaults.
    /// </summary>
    private static readonly Func<object, byte[]> DefaultSerializer =
        payload => JsonSerializer.SerializeToUtf8Bytes(payload, Json.GetTypeInfo(payload.GetType()));

    /// <summary>
    /// The deserializer every options instance starts with: the declared payload type with the serializer's defaults.
    /// </summary>
    private static readonly Func<byte[], Type, object?> DefaultDeserializer =
        (bytes, type) => JsonSerializer.Deserialize(bytes, Json.GetTypeInfo(type));

    /// <summary>
    /// How long a publish waits for the client to recover a lost connection when nothing else is configured.
    /// </summary>
    /// <remarks>
    /// The client tries to recover every NetworkRecoveryInterval, five seconds by default, so the wait covers six
    /// attempts: about what a broker restart or a node failover takes.
    /// </remarks>
    internal static readonly TimeSpan DefaultRecoveryWait = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets the connection factory used to create RabbitMQ connections.
    /// </summary>
    internal ConnectionFactory ConnectionFactory { get; } = new();

    /// <summary>
    /// Gets or sets how long a publish waits for the client to recover a lost connection; zero fails at once.
    /// </summary>
    internal TimeSpan RecoveryWait { get; set; } = DefaultRecoveryWait;

    /// <summary>
    /// Gets or sets the function used to serialize objects into byte arrays.
    /// The default implementation uses <see cref="JsonSerializer.SerializeToUtf8Bytes(object, System.Type, JsonSerializerOptions?)"/>.
    /// </summary>
    internal Func<object, byte[]> Serializer { get; set; } = DefaultSerializer;

    /// <summary>
    /// Gets or sets the function used to deserialize byte arrays into objects of a specified type.
    /// The default implementation uses <see cref="JsonSerializer"/>.
    /// </summary>
    internal Func<byte[], Type, object?> Deserializer { get; set; } = DefaultDeserializer;

    /// <summary>
    /// Gets whether messages are still serialized by the default serializer, which can write into a pooled buffer.
    /// </summary>
    internal bool HasDefaultSerializer => ReferenceEquals(Serializer, DefaultSerializer);

    /// <summary>
    /// Serializes a message for publishing: into a rented array with the default serializer, through the configured one otherwise.
    /// </summary>
    /// <param name="payload">The message to serialize.</param>
    /// <returns>The body, to be disposed once the publish that reads it has completed.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the configured serializer returned null.</exception>
    internal RentedBody Serialize(object payload)
    {
        if (HasDefaultSerializer)
        {
            return JsonBody.Write(payload, Json);
        }

        var bytes = Serializer(payload)
            ?? throw new InvalidOperationException($"The configured serializer returned null for a {payload.GetType()}");

        return new RentedBody(bytes);
    }

    /// <summary>
    /// Deserializes a delivered body: straight from the delivered memory with the default deserializer, from a copy
    /// handed to the configured one otherwise.
    /// </summary>
    /// <param name="body">The delivered body, valid only while the delivery is handled.</param>
    /// <param name="type">The payload type of the consumer declaration.</param>
    /// <returns>The payload, or null when the deserializer returned null.</returns>
    internal object? Deserialize(ReadOnlyMemory<byte> body, Type type) =>
        ReferenceEquals(Deserializer, DefaultDeserializer)
            ? JsonSerializer.Deserialize(body.Span, Json.GetTypeInfo(type))
            : Deserializer(body.ToArray(), type);

    /// <summary>
    /// Gets a list of connection declarations.
    /// </summary>
    internal IList<RabbitConnectionDeclaration> ConnectionDeclarations { get; } = [];

    /// <summary>
    /// Gets a list of exchange declarations.
    /// </summary>
    internal IList<RabbitExchangeDeclaration> ExchangeDeclarations { get; } = [];

    /// <summary>
    /// Gets a list of queue declarations.
    /// </summary>
    internal IList<RabbitQueueDeclaration> QueueDeclarations { get; } = [];

    /// <summary>
    /// Gets a list of consumer declarations.
    /// </summary>
    internal IList<RabbitConsumerDeclaration> ConsumerDeclarations { get; } = [];

    /// <summary>
    /// Gets a dictionary of producer declarations, keyed by the type of the produced message.
    /// </summary>
    internal IDictionary<Type, RabbitProducerDeclaration> ProducerDeclarations { get; } = new Dictionary<Type, RabbitProducerDeclaration>();

    /// <summary>
    /// Creates the default serializer's options, with the reflection resolver only where reflection is allowed.
    /// </summary>
    /// <returns>Options equal to the serializer's defaults.</returns>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault, which is false when trimming.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault, which is false ahead of time.")]
    private static JsonSerializerOptions Reflective()
    {
        var options = new JsonSerializerOptions();

        if (JsonSerializer.IsReflectionEnabledByDefault)
            options.TypeInfoResolver = new DefaultJsonTypeInfoResolver();

        return options;
    }
}
