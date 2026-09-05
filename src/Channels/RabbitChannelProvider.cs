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
    Task<RabbitChannel> FromDeclaration(RabbitProducerDeclaration declaration,
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
	private readonly ConcurrentDictionary<ChannelKey, RabbitChannel> _channels = new();

	/// <inheritdoc/>
    public async Task<RabbitChannel> FromDeclaration(RabbitProducerDeclaration declaration,
	    CancellationToken cancellationToken = default)
    {
        var key = new ChannelKey(declaration.ConnectionDeclaration.Name, declaration.Transactional);

        if (_channels.TryGetValue(key, out var existing))
        {
            if (existing.IsOpen)
            {
                return existing;
            }

            _channels.TryRemove(new KeyValuePair<ChannelKey, RabbitChannel>(key, existing));
        }

        var connection = await connectionProvider.FromDeclaration(declaration.ConnectionDeclaration, cancellationToken);

        // Confirmations and transactions are mutually exclusive on a channel
        var channel = await connection.CreateChannelAsync(
            publisherConfirmations: !declaration.Transactional,
            cancellationToken);

        EnsureLogging(channel);
        EnsureEviction(key, channel);

        if (declaration.Transactional)
        {
            await channel.TxSelectAsync(cancellationToken);
        }

        // The channel is fully initialized before publication; a concurrent loser is disposed
        var winner = _channels.GetOrAdd(key, channel);
        if (!ReferenceEquals(winner, channel))
        {
            channel.Dispose();
        }

        return winner;
    }

    /// <summary>
    /// Disposes every cached channel so the broker sees a clean shutdown.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        foreach (var key in _channels.Keys)
        {
            if (_channels.TryRemove(key, out var channel))
            {
                await channel.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Evicts the channel from the cache the moment the broker closes it.
    /// </summary>
    /// <param name="key">The cache key the channel is stored under.</param>
    /// <param name="channel">The channel to watch.</param>
    /// <remarks>
    /// Removal is instance-checked, so a replacement channel that already took the key is never
    /// evicted by its dead predecessor's shutdown event.
    /// </remarks>
    private void EnsureEviction(ChannelKey key, RabbitChannel channel)
    {
        channel.ChannelShutdownAsync += (_, _) =>
        {
            _channels.TryRemove(new KeyValuePair<ChannelKey, RabbitChannel>(key, channel));
            return Task.CompletedTask;
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
		    logger.LogError(
			    args.Exception,
			    "Channel '{ChannelNumber}' exception. Connection: '{Name}'",
			    channel.ChannelNumber, channel.ConnectionDeclaration.Name);
		    return Task.CompletedTask;
	    };

	    channel.FlowControlAsync += (_, args) =>
	    {
		    logger.LogWarning(
			    "Channel '{ChannelNumber}' flow control. Connection: '{Name}', active: {Active}",
			    channel.ChannelNumber, channel.ConnectionDeclaration.Name, args.Active);
		    return Task.CompletedTask;
	    };

        channel.ChannelShutdownAsync += (_, args) =>
        {
	        logger.LogError(
		        "Channel '{ChannelNumber}' shutdown. Connection: '{Name}', reason: '{ReplyText}'",
		        channel.ChannelNumber, channel.ConnectionDeclaration.Name, args.ReplyText);
	        return Task.CompletedTask;
        };
    }
}
