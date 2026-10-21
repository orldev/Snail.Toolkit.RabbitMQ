using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Consumers;

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
    : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Gets the client-provided name for this connection, falling back to the declaration name.
    /// </summary>
    public string ClientProvidedName => connection.ClientProvidedName ?? ConnectionDeclaration.Name;

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
    public Task<RabbitChannel> CreateChannelAsync(
        bool publisherConfirmations,
        CancellationToken cancellationToken = default)
    {
        var options = publisherConfirmations
            ? new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)
            : null;

        return OpenAsync(options, publisherConfirmations, cancellationToken);
    }

    /// <summary>
    /// Waits until the client has recovered the connection, for at most the given time.
    /// </summary>
    /// <param name="wait">How long to wait for the recovery.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns><c>true</c> when the connection is open again; <c>false</c> when the wait ran out first.</returns>
    /// <remarks>
    /// The client recovers a lost connection by itself, retrying every network recovery interval, and raises its
    /// recovery event once it is back. The connection is asked again after subscribing, so a recovery that completes
    /// in between is not missed.
    /// </remarks>
    internal async Task<bool> HasRecoveredAsync(TimeSpan wait, CancellationToken cancellationToken)
    {
        if (connection.IsOpen)
        {
            return true;
        }

        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncEventHandler<AsyncEventArgs> onRecovered = (_, _) =>
        {
            recovered.TrySetResult();
            return Task.CompletedTask;
        };
        connection.RecoverySucceededAsync += onRecovered;

        try
        {
            if (connection.IsOpen)
            {
                return true;
            }

            await recovered.Task.WaitAsync(wait, cancellationToken);

            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
        finally
        {
            connection.RecoverySucceededAsync -= onRecovered;
        }
    }

    /// <summary>
    /// Creates the channel a consumer declaration consumes on, with as many dispatcher workers as it has consumers.
    /// </summary>
    /// <param name="declaration">The consumer declaration the channel serves.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the created <see cref="RabbitChannel"/>.</returns>
    /// <remarks>
    /// The client dispatches the deliveries of one channel through <c>ConsumerDispatchConcurrency</c> workers and
    /// defaults to one, so several consumers on one channel would still be handled one delivery at a time. Measured:
    /// with Count(2), Prefetch(1) and two messages waiting, one handler ran at a time (chaos test, 2026-10-21).
    /// </remarks>
    internal Task<RabbitChannel> CreateChannelAsync(
        RabbitConsumerDeclaration declaration,
        CancellationToken cancellationToken)
    {
        var workers = (ushort)Math.Min(declaration.Count, ushort.MaxValue);
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: workers);

        return OpenAsync(options, tracksConfirmations: false, cancellationToken);
    }

    private async Task<RabbitChannel> OpenAsync(
        CreateChannelOptions? options,
        bool tracksConfirmations,
        CancellationToken cancellationToken)
    {
        var channel = await connection.CreateChannelAsync(options, cancellationToken);

        var opened = new RabbitChannel(ConnectionDeclaration, channel, logger)
        {
            TracksConfirmations = tracksConfirmations
        };

        return opened;
    }

    /// <summary>
    /// Releases all resources used by the RabbitMQ connection.
    /// </summary>
    public void Dispose()
    {
        connection.Dispose();
    }

    /// <summary>
    /// Asynchronously releases all resources used by the RabbitMQ connection.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        return connection.DisposeAsync();
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
