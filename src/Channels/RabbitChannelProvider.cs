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
	/// Gets a cached, thread-safe channel configured according to the producer declaration.
	/// </summary>
	/// <param name="declaration">The producer declaration containing channel configuration.</param>
	/// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
	/// <returns>A task that represents the asynchronous operation, containing the configured <see cref="RabbitChannel"/>.</returns>
	/// <remarks>
	/// Channels are cached based on payload type, connection name, and exchange/routing key combination.
	/// Transactional channels are automatically initialized with transaction support.
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
	: IRabbitChannelProvider
{
	private readonly ConcurrentDictionary<(Type, string, string), RabbitChannel> _channels = new();

	/// <inheritdoc/>
    public async Task<RabbitChannel> FromDeclaration(RabbitProducerDeclaration declaration,
	    CancellationToken cancellationToken = default)
    {
        var key = (declaration.PayloadType, declaration.ConnectionDeclaration.Name, declaration.ExchangeDeclaration?.Name ?? declaration.RoutingKey ?? string.Empty);
	    var connection = await connectionProvider.FromDeclaration(declaration.ConnectionDeclaration, cancellationToken);

        if (_channels.TryGetValue(key, out var existing))
        {
            return existing;
        }

        // Confirmations and transactions are mutually exclusive on a channel
        var channel = await connection.CreateChannelAsync(
            publisherConfirmations: !declaration.Transactional,
            cancellationToken);

        EnsureLogging(channel);

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