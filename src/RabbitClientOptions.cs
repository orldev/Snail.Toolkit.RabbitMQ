using RabbitMQ.Client;

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
    /// Gets or sets the port of the RabbitMQ server to connect to.
    /// </summary>
    /// <value>
    /// The default is <see cref="AmqpTcpEndpoint.UseDefaultPort"/>, which resolves to 5672
    /// for plain connections and 5671 once <see cref="Ssl"/> is enabled.
    /// </value>
    public int Port { get; set; } = AmqpTcpEndpoint.UseDefaultPort;

    /// <summary>
    /// Gets or sets the virtual host to connect to.
    /// The default value is "/".
    /// </summary>
    public string VirtualHost { get; set; } = "/";

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

    /// <summary>
    /// Gets or sets the TLS settings for the connection.
    /// </summary>
    /// <remarks>TLS is off unless <see cref="RabbitClientSslOptions.Enabled"/> is set.</remarks>
    public RabbitClientSslOptions Ssl { get; set; } = new();
}

/// <summary>
/// Provides the TLS settings of a RabbitMQ client connection.
/// </summary>
public class RabbitClientSslOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether the connection uses TLS.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the server name expected on the broker's certificate.
    /// </summary>
    /// <remarks>
    /// Left unset, it falls back to <see cref="RabbitClientOptions.HostName"/>. The two differ
    /// when the broker is reached through an address that is not the name it was issued for —
    /// a container name, a load balancer, an IP — and a mismatch fails the handshake.
    /// </remarks>
    public string? ServerName { get; set; }

    /// <summary>
    /// Gets or sets the path to a client certificate presented to the broker.
    /// </summary>
    /// <remarks>Only needed for mutual TLS, where the broker authenticates the client too.</remarks>
    public string? CertPath { get; set; }

    /// <summary>
    /// Gets or sets the passphrase of the client certificate at <see cref="CertPath"/>.
    /// </summary>
    public string? CertPassphrase { get; set; }
}
