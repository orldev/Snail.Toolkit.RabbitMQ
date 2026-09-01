namespace Snail.Toolkit.RabbitMQ.Connections;

/// <summary>
/// Represents the configuration declaration for a RabbitMQ connection.
/// </summary>
/// <param name="name">The unique name identifying this connection configuration.</param>
/// <remarks>
/// This class serves as a container for RabbitMQ connection settings and parameters.
/// The connection name is used to identify and reference the connection throughout the application.
/// </remarks>
public class RabbitConnectionDeclaration(string name)
{
    /// <summary>
    /// Gets the unique name identifying this connection configuration.
    /// </summary>
    /// <value>
    /// The name that uniquely identifies this RabbitMQ connection configuration.
    /// This name is used when referencing the connection from other components.
    /// </value>
    public string Name { get; } = name;
}