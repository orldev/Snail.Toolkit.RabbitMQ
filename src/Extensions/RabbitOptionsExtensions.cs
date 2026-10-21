using RabbitMQ.Client;

namespace Snail.Toolkit.RabbitMQ.Extensions;

/// <summary>
/// Provides extension methods for configuring <see cref="RabbitOptions"/>.
/// </summary>
public static class RabbitOptionsExtensions
{
    /// <summary>
    /// Configures the underlying RabbitMQ connection factory.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="factory">An action to configure the <see cref="ConnectionFactory"/>.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <remarks>
    /// This method provides direct access to the RabbitMQ client's connection factory,
    /// allowing complete control over connection parameters like host, port, credentials, etc.
    /// </remarks>
    /// <example>
    /// <code>
    /// options.UseConnectionFactory(factory => {
    ///     factory.HostName = "rabbit.example.com";
    ///     factory.Port = 5672;
    ///     factory.UserName = "guest";
    ///     factory.Password = "guest";
    /// });
    /// </code>
    /// </example>
    public static RabbitOptions UseConnectionFactory(
        this RabbitOptions options,
        Action<ConnectionFactory> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        factory(options.ConnectionFactory);
        return options;
    }

    /// <summary>
    /// Configures custom serialization for message payloads.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="serializer">A function that serializes objects to byte arrays.</param>
    /// <param name="deserializer">A function that deserializes byte arrays back to objects.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if either serializer or deserializer is null.</exception>
    /// <remarks>
    /// The serializer will be used when publishing messages, and the deserializer when consuming messages.
    /// Common serialization formats include JSON, MessagePack, or Protocol Buffers.
    /// </remarks>
    public static RabbitOptions UseSerialization(
        this RabbitOptions options,
        Func<object, byte[]> serializer,
        Func<byte[], Type, object> deserializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(deserializer);

        options.Serializer = serializer;
        options.Deserializer = deserializer;

        return options;
    }

    /// <summary>
    /// Configures the connection using a RabbitMQ connection URI.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="url">The connection URI in format amqp://user:pass@host:port/vhost.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the URL is null.</exception>
    /// <exception cref="UriFormatException">Thrown if the URL is invalid.</exception>
    /// <remarks>
    /// The URI should follow the AMQP URI scheme specification.
    /// This provides a convenient way to configure all connection parameters in one string.
    /// </remarks>
    public static RabbitOptions UseConnectionUrl(
        this RabbitOptions options,
        string url)
    {
        ArgumentNullException.ThrowIfNull(url);

        return options.UseConnectionUrl(new Uri(url));
    }

    /// <summary>
    /// Configures the connection using a RabbitMQ connection URI.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="url">The connection URI in format amqp://user:pass@host:port/vhost.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the URL is null.</exception>
    public static RabbitOptions UseConnectionUrl(
        this RabbitOptions options,
        Uri url)
    {
        ArgumentNullException.ThrowIfNull(url);

        return options.UseConnectionFactory(factory => factory.Uri = url);
    }

    /// <summary>
    /// Sets how long a publish waits for the client to recover a lost connection before it fails.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="wait">The longest wait; <see cref="TimeSpan.Zero"/> makes a publish on a lost connection fail at once.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the wait is negative or longer than <see cref="int.MaxValue"/> milliseconds.</exception>
    /// <remarks>
    /// Thirty seconds unless set: the client retries the recovery every network recovery interval, five seconds by
    /// default, so the wait covers six attempts. A publish waits only for a connection the client recovers by itself
    /// (automatic recovery is on unless the connection factory turns it off) and only when the connection was lost,
    /// not when the broker closed the channel for a protocol error. After the recovery the message is published once
    /// more, so a message the broker received just before the connection dropped may arrive twice.
    /// </remarks>
    public static RabbitOptions UseRecoveryWait(
        this RabbitOptions options,
        TimeSpan wait)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(wait, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(wait, TimeSpan.FromMilliseconds(int.MaxValue));

        options.RecoveryWait = wait;

        return options;
    }

    /// <summary>
    /// Sets the client-provided name for the connection.
    /// </summary>
    /// <param name="options">The RabbitMQ options to configure.</param>
    /// <param name="clientProvidedName">The name to identify this connection.</param>
    /// <returns>The configured <see cref="RabbitOptions"/> for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the name is null.</exception>
    /// <remarks>
    /// This name will appear in the RabbitMQ management UI and logs,
    /// making it easier to identify different application connections.
    /// </remarks>
    public static RabbitOptions UseClientProvidedName(
        this RabbitOptions options,
        string clientProvidedName)
    {
        ArgumentNullException.ThrowIfNull(clientProvidedName);
        return options.UseConnectionFactory(factory => factory.ClientProvidedName = clientProvidedName);
    }
}
