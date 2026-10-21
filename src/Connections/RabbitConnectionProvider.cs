using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Channels;

namespace Snail.Toolkit.RabbitMQ.Connections;

/// <summary>
/// Provides thread-safe, cached RabbitMQ connections based on connection declarations.
/// </summary>
public interface IRabbitConnectionProvider
{
    /// <summary>
    /// Gets or creates a cached, thread-safe RabbitMQ connection based on the specified declaration.
    /// </summary>
    /// <param name="connection">The connection declaration containing configuration parameters.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the configured <see cref="RabbitConnection"/>.</returns>
    /// <remarks>
    /// Connections are cached by their declaration name for efficient reuse.
    /// Concurrent first calls for one name share a single connection attempt,
    /// so racing callers can never open and leak a second TCP connection.
    /// </remarks>
    Task<RabbitConnection> FromDeclarationAsync(RabbitConnectionDeclaration connection,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of <see cref="IRabbitConnectionProvider"/> that manages connection caching and lifecycle.
/// </summary>
/// <param name="channelLogger">Logger for channel-related events.</param>
/// <param name="connectionLogger">Logger for connection-related events.</param>
/// <param name="options">Configuration options for RabbitMQ connections.</param>
internal sealed class RabbitConnectionProvider(
    ILogger<RabbitChannel> channelLogger,
    ILogger<RabbitConnection> connectionLogger,
    IOptions<RabbitOptions> options)
    : IRabbitConnectionProvider, IAsyncDisposable
{
    private readonly RabbitOptions _options = options.Value;
    private readonly ConcurrentDictionary<string, Lazy<Task<RabbitConnection>>> _connections = new();
    private readonly CancellationTokenSource _closing = new();

    /// <inheritdoc/>
    /// <remarks>
    /// The open itself runs under the provider's own token, never under a caller's: the first caller's token in the
    /// shared task would cancel every other caller waiting on the same connection. Each caller waits with its own
    /// token instead, and an open that outlives an impatient caller still lands in the cache.
    /// </remarks>
    public async Task<RabbitConnection> FromDeclarationAsync(RabbitConnectionDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        var opening = _connections.GetOrAdd(
            declaration.Name,
            _ => new Lazy<Task<RabbitConnection>>(() => OpenAsync(declaration, _closing.Token)));

        try
        {
            return await opening.Value.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _connections.TryRemove(new KeyValuePair<string, Lazy<Task<RabbitConnection>>>(declaration.Name, opening));
            throw;
        }
    }

    /// <summary>
    /// Disposes every established connection so the broker sees a clean shutdown; an open still in flight is
    /// cancelled, and one that completes anyway is disposed rather than left behind.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        await _closing.CancelAsync();

        foreach (var name in _connections.Keys)
        {
            if (!_connections.TryRemove(name, out var opening) || !opening.IsValueCreated)
            {
                continue;
            }

            await DisposeAsync(opening.Value);
        }

        _closing.Dispose();
    }

    private static async Task DisposeAsync(Task<RabbitConnection> opening)
    {
        RabbitConnection opened;
        try
        {
            opened = await opening;
        }
        catch (Exception)
        {
            return;
        }

        await opened.DisposeAsync();
    }

    /// <summary>
    /// Opens one RabbitMQ connection for the declaration and wires up its event logging.
    /// </summary>
    /// <param name="declaration">The connection declaration to open.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the established connection.</returns>
    private async Task<RabbitConnection> OpenAsync(RabbitConnectionDeclaration declaration,
        CancellationToken cancellationToken)
    {
        var clientProvidedName = _options.ConnectionFactory.ClientProvidedName is null
            ? declaration.Name
            : $"{_options.ConnectionFactory.ClientProvidedName}.{declaration.Name}";

        var connection = new RabbitConnection(
            channelLogger,
            declaration,
            await _options.ConnectionFactory.CreateConnectionAsync(clientProvidedName, cancellationToken));

        EnsureLogging(connection);

        return connection;
    }

    /// <summary>
    /// Configures event logging for connection lifecycle events.
    /// </summary>
    /// <param name="connection">The connection to configure logging for.</param>
    private void EnsureLogging(RabbitConnection connection)
    {
        Log.ConnectionEstablished(connectionLogger, connection.ClientProvidedName);

        connection.CallbackExceptionAsync += (_, args) =>
        {
            Log.ConnectionCallbackFailed(connectionLogger, args.Exception, connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.ConnectionBlockedAsync += (_, args) =>
        {
            Log.ConnectionBlocked(connectionLogger, connection.ConnectionDeclaration.Name, args.Reason);
            return Task.CompletedTask;
        };

        connection.ConnectionShutdownAsync += (_, args) =>
        {
            Log.ConnectionShutdown(connectionLogger, connection.ConnectionDeclaration.Name, args.Cause);
            return Task.CompletedTask;
        };

        connection.ConnectionUnblockedAsync += (_, args) =>
        {
            Log.ConnectionUnblocked(connectionLogger, connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.RecoverySucceededAsync += (_, args) =>
        {
            Log.ConnectionRecovered(connectionLogger, connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.ConnectionRecoveryErrorAsync += (_, args) =>
        {
            Log.ConnectionRecoveryFailed(connectionLogger, args.Exception, connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };
    }
}
