namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Provides options for configuring a RabbitMQ client connection.
/// </summary>
public class RabbitClientOptions
{
    /// <summary>
    /// Gets or sets the host name of the RabbitMQ server to connect to.
    /// The default value is "localhost".
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the username to use when authenticating to the RabbitMQ server.
    /// The default value is "guest".
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the password to use when authenticating to the RabbitMQ server.
    /// The default value is "guest".
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the default client-provided name to be used for RabbitMQ connections.
    /// This name can be useful for identifying connections in the RabbitMQ management interface.
    /// The default value is "default".
    /// </summary>
    public string ClientProvidedName { get; set; } = "default";
}