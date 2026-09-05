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
    Task<RabbitConnection> FromDeclaration(RabbitConnectionDeclaration connection,
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

    /// <inheritdoc/>
    public async Task<RabbitConnection> FromDeclaration(RabbitConnectionDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        var opening = _connections.GetOrAdd(
            declaration.Name,
            _ => new Lazy<Task<RabbitConnection>>(() => OpenAsync(declaration, cancellationToken)));

        try
        {
            return await opening.Value;
        }
        catch
        {
            _connections.TryRemove(new KeyValuePair<string, Lazy<Task<RabbitConnection>>>(declaration.Name, opening));
            throw;
        }
    }

    /// <summary>
    /// Disposes every established connection so the broker sees a clean shutdown.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        foreach (var name in _connections.Keys)
        {
            if (!_connections.TryRemove(name, out var opening) || !opening.IsValueCreated)
            {
                continue;
            }

            if (opening.Value is { IsCompletedSuccessfully: true } opened)
            {
                await (await opened).DisposeAsync();
            }
        }
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
        connectionLogger.LogInformation("Connection '{ClientProvidedName}' established",
            connection.ClientProvidedName);

        connection.CallbackExceptionAsync += (_, args) =>
        {
            connectionLogger.LogError(
                args.Exception,
                "Connection: {Name}", connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.ConnectionBlockedAsync += (_, args) =>
        {
            connectionLogger.LogError(
                "Connection '{Name}' blocked: {Reason}",
                connection.ConnectionDeclaration.Name, args.Reason);
            return Task.CompletedTask;
        };

        connection.ConnectionShutdownAsync += (_, args) =>
        {
            connectionLogger.LogInformation(
                "Connection '{Name}' shutdown: {Cause}",
                connection.ConnectionDeclaration.Name, args.Cause);
            return Task.CompletedTask;
        };

        connection.ConnectionUnblockedAsync += (_, args) =>
        {
            connectionLogger.LogInformation(
                "Connection '{Name}' unblocked",
                connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.RecoverySucceededAsync += (_, args) =>
        {
            connectionLogger.LogInformation(
                "Connection '{Name}' recovered",
                connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        connection.ConnectionRecoveryErrorAsync += (_, args) =>
        {
            connectionLogger.LogError(args.Exception,
                "Connection '{Name}' recovery error",
                connection.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };
    }
}
