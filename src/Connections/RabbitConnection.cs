using System.Reflection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Snail.Toolkit.RabbitMQ.Channels;

namespace Snail.Toolkit.RabbitMQ.Connections;

/// <summary>
/// Represents a managed RabbitMQ connection that provides channel creation and event handling capabilities.
/// </summary>
/// <param name="logger">The logger instance for channel-related events.</param>
/// <param name="connectionDeclaration">The configuration declaration for this connection.</param>
/// <param name="connection">The underlying RabbitMQ connection.</param>
public sealed class RabbitConnection(
    ILogger<RabbitChannel> logger,
    RabbitConnectionDeclaration connectionDeclaration,
    IConnection connection)
    : IDisposable
{
    /// <summary>
    /// Gets the client-provided name for this connection, falling back to the assembly name or "default" if not specified.
    /// </summary>
    public string ClientProvidedName => connection.ClientProvidedName ?? Assembly.GetCallingAssembly().GetName().Name ?? "default";

    /// <summary>
    /// Gets the connection declaration containing the configuration for this connection.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    /// <summary>
    /// Creates a new channel on this RabbitMQ connection.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the created <see cref="RabbitChannel"/>.</returns>
    public Task<RabbitChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        return CreateChannelAsync(publisherConfirmations: false, cancellationToken);
    }

    /// <summary>
    /// Creates a new channel on this RabbitMQ connection, optionally with publisher confirmations.
    /// </summary>
    /// <param name="publisherConfirmations">
    /// When true, the channel is created with publisher confirmations enabled and tracked:
    /// publishing completes only after the broker confirms the message and throws when it is nacked.
    /// </param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the created <see cref="RabbitChannel"/>.</returns>
    /// <remarks>Publisher confirmations cannot be combined with transactions on the same channel.</remarks>
    public async Task<RabbitChannel> CreateChannelAsync(
        bool publisherConfirmations,
        CancellationToken cancellationToken = default)
    {
        var channel = await connection.CreateChannelAsync(
            publisherConfirmations
                ? new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true)
                : null,
            cancellationToken);

        return new RabbitChannel(
            ConnectionDeclaration,
            channel,
            logger);
    }

    /// <summary>
    /// Releases all resources used by the RabbitMQ connection.
    /// </summary>
    public void Dispose()
    {
        connection.Dispose();
    }

    /// <summary>
    /// Occurs when a callback exception is thrown.
    /// </summary>
    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync
    {
        add => connection.CallbackExceptionAsync += value;
        remove => connection.CallbackExceptionAsync -= value;
    }
    
    /// <summary>
    /// Occurs when connection recovery succeeds.
    /// </summary>
    public event AsyncEventHandler<AsyncEventArgs> RecoverySucceededAsync
    {
        add => connection.RecoverySucceededAsync += value;
        remove => connection.RecoverySucceededAsync -= value;
    }
    
    /// <summary>
    /// Occurs when an error happens during connection recovery.
    /// </summary>
    public event AsyncEventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryErrorAsync
    {
        add => connection.ConnectionRecoveryErrorAsync += value;
        remove => connection.ConnectionRecoveryErrorAsync -= value;
    }
    
    /// <summary>
    /// Occurs when the connection gets blocked.
    /// </summary>
    public event AsyncEventHandler<ConnectionBlockedEventArgs> ConnectionBlockedAsync
    {
        add => connection.ConnectionBlockedAsync += value;
        remove => connection.ConnectionBlockedAsync -= value;
    }
    
    /// <summary>
    /// Occurs when the connection shuts down.
    /// </summary>
    public event AsyncEventHandler<ShutdownEventArgs> ConnectionShutdownAsync
    {
        add => connection.ConnectionShutdownAsync += value;
        remove => connection.ConnectionShutdownAsync -= value;
    }
    
    /// <summary>
    /// Occurs when the connection gets unblocked.
    /// </summary>
    public event AsyncEventHandler<AsyncEventArgs> ConnectionUnblockedAsync
    {
        add => connection.ConnectionUnblockedAsync += value;
        remove => connection.ConnectionUnblockedAsync -= value;
    }
}