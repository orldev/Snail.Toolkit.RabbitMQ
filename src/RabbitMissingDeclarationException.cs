namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Represents an exception thrown when a producer declaration is missing for a specific payload type.
/// </summary>
public class RabbitMissingDeclarationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMissingDeclarationException"/> class with a specified payload type.
    /// </summary>
    /// <param name="payloadType">The <see cref="Type"/> of the payload for which the producer declaration is missing.</param>
    public RabbitMissingDeclarationException(Type payloadType)
        : base($"Missing producer declaration for payload type: {payloadType.FullName}")
    {
        PayloadType = payloadType;
    }

    /// <summary>
    /// Gets the <see cref="Type"/> of the payload for which the producer declaration is missing.
    /// </summary>
    public Type PayloadType { get; }
}