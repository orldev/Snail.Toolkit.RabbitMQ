namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Represents an exception thrown when a producer declaration is missing for a specific payload type.
/// </summary>
/// <param name="payloadType">The <see cref="Type"/> of the payload for which the producer declaration is missing.</param>
public sealed class RabbitMissingDeclarationException(Type payloadType)
    : Exception($"Missing producer declaration for payload type: {payloadType.FullName}")
{
    /// <summary>
    /// Gets the <see cref="Type"/> of the payload for which the producer declaration is missing.
    /// </summary>
    public Type PayloadType { get; } = payloadType;
}
