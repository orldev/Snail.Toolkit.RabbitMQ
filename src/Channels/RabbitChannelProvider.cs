using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Producers;

namespace Snail.Toolkit.RabbitMQ.Channels;

/// <summary>
/// Provides thread-safe channel instances for RabbitMQ operations, managing channel lifecycle and caching.
/// </summary>
public interface IRabbitChannelProvider
{
    /// <summary>
    /// Gets a cached, open channel configured according to the producer declaration.
    /// </summary>
    /// <param name="declaration">The producer declaration containing channel configuration.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the configured <see cref="RabbitChannel"/>.</returns>
    /// <remarks>
    /// Channels are cached per connection and mode (transactional or confirmation-tracking),
    /// because those are the only pieces of state a channel carries; exchange and routing key
    /// are supplied per publish. A channel closed by the broker is evicted and replaced on the
    /// next call, so one protocol error never disables publishing permanently.
    /// </remarks>
    Task<RabbitChannel> FromDeclarationAsync(RabbitProducerDeclaration declaration,
         CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of <see cref="IRabbitChannelProvider"/> that manages channel caching and lifecycle.
/// </summary>
/// <param name="logger">The logger instance for channel event logging.</param>
/// <param name="connectionProvider">The connection provider for creating underlying RabbitMQ connections.</param>
internal sealed class RabbitChannelProvider(
    ILogger<RabbitChannel> logger,
    IRabbitConnectionProvider connectionProvider)
    : IRabbitChannelProvider, IAsyncDisposable
{
    private readonly ConcurrentDictionary<ChannelKey, OpenChannel> _channels = new();

    /// <inheritdoc/>
    /// <remarks>
    /// Confirmations and transactions are mutually exclusive on a channel. A channel is fully initialized before it is
    /// published to the cache; the loser of a concurrent open is disposed. An open channel is handed back as the
    /// completed task kept beside it, so a publish on a cached channel allocates nothing here.
    /// </remarks>
    public Task<RabbitChannel> FromDeclarationAsync(RabbitProducerDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        var key = new ChannelKey(declaration.ConnectionDeclaration.Name, declaration.Transactional);

        if (_channels.TryGetValue(key, out var existing))
        {
            if (existing.Channel.IsOpen)
            {
                return existing.Completed;
            }

            _channels.TryRemove(new KeyValuePair<ChannelKey, OpenChannel>(key, existing));
        }

        return OpenAsync(key, declaration, cancellationToken);
    }

    /// <summary>
    /// Disposes every cached channel so the broker sees a clean shutdown.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        foreach (var key in _channels.Keys)
        {
            if (_channels.TryRemove(key, out var open))
            {
                await open.Channel.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Opens a channel for the declaration and publishes it to the cache, unless a concurrent caller got there first.
    /// </summary>
    /// <param name="key">The cache key the channel is stored under.</param>
    /// <param name="declaration">The producer declaration containing channel configuration.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The cached channel: this call's, or the one a concurrent caller published first.</returns>
    private async Task<RabbitChannel> OpenAsync(ChannelKey key, RabbitProducerDeclaration declaration,
        CancellationToken cancellationToken)
    {
        var connection = await connectionProvider.FromDeclarationAsync(declaration.ConnectionDeclaration, cancellationToken);

        var channel = await connection.CreateChannelAsync(
            publisherConfirmations: !declaration.Transactional,
            cancellationToken);

        try
        {
            if (declaration.Transactional)
            {
                await channel.TxSelectAsync(cancellationToken);
            }
        }
        catch
        {
            await channel.DisposeAsync();
            throw;
        }

        var open = new OpenChannel(channel);
        EnsureLogging(channel);
        EnsureEviction(key, open);
        var winner = _channels.GetOrAdd(key, open);

        if (!ReferenceEquals(winner, open))
        {
            await channel.DisposeAsync();
        }

        return winner.Channel;
    }

    /// <summary>
    /// Evicts the channel from the cache the moment the broker closes it, and disposes it so the client's automatic
    /// recovery does not reopen it beside its replacement.
    /// </summary>
    /// <param name="key">The cache key the channel is stored under.</param>
    /// <param name="open">The cache entry of the channel to watch.</param>
    /// <remarks>
    /// Removal is instance-checked, so a replacement channel that already took the key is never evicted by its dead
    /// predecessor's shutdown event. The evicted channel was recorded by the client for recovery; left undisposed it
    /// came back as an idle channel on every reconnect (one before a forced close, two after; chaos test, 2026-10-21).
    /// </remarks>
    private void EnsureEviction(ChannelKey key, OpenChannel open)
    {
        open.Channel.ChannelShutdownAsync += async (_, _) =>
        {
            var evicted = _channels.TryRemove(new KeyValuePair<ChannelKey, OpenChannel>(key, open));

            if (evicted)
            {
                await open.Channel.DisposeAsync();
            }
        };
    }

    /// <summary>
    /// Configures event logging for channel lifecycle events.
    /// </summary>
    /// <param name="channel">The channel to configure logging for.</param>
    private void EnsureLogging(RabbitChannel channel)
    {
        channel.CallbackExceptionAsync += (_, args) =>
        {
            Log.ChannelCallbackFailed(logger, args.Exception, channel.ChannelNumber, channel.ConnectionDeclaration.Name);
            return Task.CompletedTask;
        };

        channel.FlowControlAsync += (_, args) =>
        {
            Log.ChannelFlowControl(logger, channel.ChannelNumber, channel.ConnectionDeclaration.Name, args.Active);
            return Task.CompletedTask;
        };

        channel.ChannelShutdownAsync += (_, args) =>
        {
            Log.ChannelShutdown(logger, channel.ChannelNumber, channel.ConnectionDeclaration.Name, args.ReplyText);
            return Task.CompletedTask;
        };
    }

    /// <summary>
    /// A cached channel and the completed task that hands it out.
    /// </summary>
    /// <param name="channel">The open channel.</param>
    private sealed class OpenChannel(RabbitChannel channel)
    {
        /// <summary>
        /// Gets the open channel.
        /// </summary>
        public RabbitChannel Channel { get; } = channel;

        /// <summary>
        /// Gets a task already completed with <see cref="Channel"/>.
        /// </summary>
        public Task<RabbitChannel> Completed { get; } = Task.FromResult(channel);
    }
}
