using Microsoft.Extensions.Logging;

namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// What this library says about its connections, channels, consumers and producers.
/// </summary>
/// <remarks>
/// Each message is generated once, so a line below the configured level costs a level check and nothing else.
/// </remarks>
internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information,
        Message = "Connection '{ClientProvidedName}' established")]
    internal static partial void ConnectionEstablished(ILogger logger, string? clientProvidedName);

    [LoggerMessage(EventId = 2, Level = LogLevel.Error,
        Message = "Connection: {Name}")]
    internal static partial void ConnectionCallbackFailed(ILogger logger, Exception exception, string name);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error,
        Message = "Connection '{Name}' blocked: {Reason}")]
    internal static partial void ConnectionBlocked(ILogger logger, string name, string reason);

    [LoggerMessage(EventId = 4, Level = LogLevel.Information,
        Message = "Connection '{Name}' shutdown: {Cause}")]
    internal static partial void ConnectionShutdown(ILogger logger, string name, object? cause);

    [LoggerMessage(EventId = 5, Level = LogLevel.Information,
        Message = "Connection '{Name}' unblocked")]
    internal static partial void ConnectionUnblocked(ILogger logger, string name);

    [LoggerMessage(EventId = 6, Level = LogLevel.Information,
        Message = "Connection '{Name}' recovered")]
    internal static partial void ConnectionRecovered(ILogger logger, string name);

    [LoggerMessage(EventId = 7, Level = LogLevel.Error,
        Message = "Connection '{Name}' recovery error")]
    internal static partial void ConnectionRecoveryFailed(ILogger logger, Exception exception, string name);

    [LoggerMessage(EventId = 8, Level = LogLevel.Error,
        Message = "Channel '{ChannelNumber}' exception. Connection: '{Name}'")]
    internal static partial void ChannelCallbackFailed(ILogger logger, Exception exception, int channelNumber, string name);

    [LoggerMessage(EventId = 9, Level = LogLevel.Warning,
        Message = "Channel '{ChannelNumber}' flow control. Connection: '{Name}', active: {Active}")]
    internal static partial void ChannelFlowControl(ILogger logger, int channelNumber, string name, bool active);

    [LoggerMessage(EventId = 10, Level = LogLevel.Error,
        Message = "Channel '{ChannelNumber}' shutdown. Connection: '{Name}', reason: '{ReplyText}'")]
    internal static partial void ChannelShutdown(ILogger logger, int channelNumber, string name, string replyText);

    [LoggerMessage(EventId = 11, Level = LogLevel.Warning,
        Message = "Unable to delete queue '{Queue}': it is still used or not empty")]
    internal static partial void QueueNotDeleted(ILogger logger, string queue);

    [LoggerMessage(EventId = 12, Level = LogLevel.Error,
        Message = "Transaction rollback failed on connection '{Name}'; the original publish failure is rethrown")]
    internal static partial void RollbackFailed(ILogger logger, Exception exception, string name);

    [LoggerMessage(EventId = 13, Level = LogLevel.Error,
        Message = "Handler failed for a message on queue '{Queue}'; the message was acknowledged on delivery and is lost")]
    internal static partial void AutoAckedMessageLost(ILogger logger, Exception exception, string queue);

    [LoggerMessage(EventId = 14, Level = LogLevel.Error,
        Message = "Poison message on queue '{Queue}': deserialization into '{PayloadType}' failed; the message will not be processed")]
    internal static partial void PoisonMessage(ILogger logger, Exception? exception, string queue, Type payloadType);

    [LoggerMessage(EventId = 15, Level = LogLevel.Error,
        Message = "Poison message on queue '{Queue}' dropped: retrying a deterministic deserialization failure is pointless and no parking queue is configured")]
    internal static partial void PoisonMessageDropped(ILogger logger, string queue);

    [LoggerMessage(EventId = 16, Level = LogLevel.Error,
        Message = "Message on queue '{Queue}' failed after {Attempt} attempt(s); parked to '{ParkingQueue}'")]
    internal static partial void MessageParked(ILogger logger, Exception exception, string queue, int attempt, string parkingQueue);

    [LoggerMessage(EventId = 17, Level = LogLevel.Error,
        Message = "Message on queue '{Queue}' failed after {Attempt} attempt(s); giving up, message dropped")]
    internal static partial void MessageDropped(ILogger logger, Exception exception, string queue, int attempt);

    [LoggerMessage(EventId = 18, Level = LogLevel.Warning,
        Message = "Message on queue '{Queue}' failed attempt {Attempt}/{MaxAttempts}; scheduled for retry")]
    internal static partial void RetryScheduled(ILogger logger, Exception exception, string queue, int attempt, int maxAttempts);

    [LoggerMessage(EventId = 19, Level = LogLevel.Error,
        Message = "Failed to park a message from queue '{Queue}' to '{ParkingQueue}'; the message is rejected for another retry cycle")]
    internal static partial void ParkingFailed(ILogger logger, Exception exception, string queue, string parkingQueue);

    [LoggerMessage(EventId = 20, Level = LogLevel.Warning,
        Message = "Failed to shut down a consumer channel cleanly; continuing with the remaining channels")]
    internal static partial void ConsumerShutdownFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 21, Level = LogLevel.Warning,
        Message = "Settling a delivery on queue '{Queue}' failed; the broker redelivers it once the channel is closed")]
    internal static partial void SettlementFailed(ILogger logger, Exception exception, string queue);

    [LoggerMessage(EventId = 22, Level = LogLevel.Warning,
        Message = "A publish on connection '{Name}' found the connection lost; waiting up to {Wait} for the client to recover it")]
    internal static partial void PublishWaitingForRecovery(ILogger logger, Exception exception, string name, TimeSpan wait);
}
