using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Exchanges;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Channels;

/// <summary>
/// Represents a RabbitMQ channel wrapper that provides higher-level operations for exchanges, queues, producers and consumers.
/// </summary>
/// <param name="connectionDeclaration">The connection declaration this channel belongs to.</param>
/// <param name="channel">The underlying RabbitMQ channel.</param>
/// <param name="logger">The logger instance for logging channel operations.</param>
public sealed class RabbitChannel(
    RabbitConnectionDeclaration connectionDeclaration,
    IChannel channel,
    ILogger<RabbitChannel> logger)
    : IDisposable
{
    /// <summary>
    /// Gets the channel number assigned by the RabbitMQ server.
    /// </summary>
    public int ChannelNumber => channel.ChannelNumber;
    
    /// <summary>
    /// Gets the connection declaration associated with this channel.
    /// </summary>
    public RabbitConnectionDeclaration ConnectionDeclaration { get; } = connectionDeclaration;

    #region Exchanges

    /// <summary>
    /// Declares an exchange on the RabbitMQ server.
    /// </summary>
    /// <param name="declaration">The exchange declaration containing configuration parameters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task ExchangeDeclareAsync(
        RabbitExchangeDeclaration declaration, 
        CancellationToken cancellationToken = default)
    {
	    await channel.ExchangeDeclareAsync(
		    exchange: declaration.Name,
		    type: declaration.Type,
		    durable: declaration.Durable,
		    autoDelete: declaration.AutoDelete,
		    arguments: declaration.Arguments,
		    passive: false,
		    noWait: declaration.NoWait,
            cancellationToken: cancellationToken);
    }
    
    /// <summary>
    /// Binds an exchange to another exchange.
    /// </summary>
    /// <param name="declaration">The destination exchange declaration.</param>
    /// <param name="binding">The binding declaration containing source exchange and routing information.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task ExchangeBindAsync(
        RabbitExchangeDeclaration declaration,
	    RabbitExchangeBindingDeclaration binding,
        CancellationToken cancellationToken = default)
    {
	    var routingKey = binding.RoutingKey ?? declaration.Name;
	    
	    await channel.ExchangeBindAsync(
		    destination: declaration.Name,
		    source: binding.ExchangeDeclaration.Name,
		    routingKey: routingKey,
		    arguments: binding.Arguments,
		    noWait: binding.NoWait,
            cancellationToken: cancellationToken);
    }
    
    /// <summary>
    /// Unbinds an exchange from another exchange.
    /// </summary>
    /// <param name="declaration">The destination exchange declaration.</param>
    /// <param name="binding">The binding declaration containing source exchange and routing information.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task ExchangeUnbindAsync(
        RabbitExchangeDeclaration declaration,
    	RabbitExchangeBindingDeclaration binding,
        CancellationToken cancellationToken = default)
    {
    	var routingKey = binding.RoutingKey ?? declaration.Name;
    
	    await channel.ExchangeUnbindAsync(
		    destination: declaration.Name,
		    source: binding.ExchangeDeclaration.Name,
		    routingKey: routingKey,
		    arguments: binding.Arguments,
		    noWait: binding.NoWait,
            cancellationToken: cancellationToken);
    }
    
    /// <summary>
    /// Deletes an exchange from the RabbitMQ server.
    /// </summary>
    /// <param name="declaration">The exchange declaration to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// If UnusedOnly is set in the declaration, attempts to delete only unused exchanges.
    /// </remarks>
    public async Task ExchangeDeleteAsync(
        RabbitExchangeDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        // TODO: try-catch when unused only failed?
        await channel.ExchangeDeleteAsync(
            declaration.Name,
            declaration.UnusedOnly,
            declaration.NoWait,
            cancellationToken: cancellationToken);
    }
    #endregion

    #region Queues

    /// <summary>
    /// Declares a queue on the RabbitMQ server.
    /// </summary>
    /// <param name="declaration">The queue declaration containing configuration parameters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task QueueDeclareAsync(
        RabbitQueueDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        await channel.QueueDeclareAsync(
            queue: declaration.Name,
            durable: declaration.Durable,
            exclusive: declaration.Exclusive,
            autoDelete: declaration.AutoDelete, 
            arguments: declaration.Arguments,
            noWait: declaration.NoWait,
            cancellationToken: cancellationToken);
    }
    
    /// <summary>
    /// Binds a queue to an exchange.
    /// </summary>
    /// <param name="declaration">The queue declaration.</param>
    /// <param name="binding">The binding declaration containing exchange and routing information.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task QueueBindAsync(
        RabbitQueueDeclaration declaration, 
        RabbitQueueBindingDeclaration binding,
        CancellationToken cancellationToken = default)
    {
        await channel.QueueBindAsync(
            queue: declaration.Name,
            exchange: binding.ExchangeDeclaration.Name,
            routingKey: binding.RoutingKey ?? declaration.Name,
            arguments: binding.Arguments,
            noWait: binding.NoWait,
            cancellationToken: cancellationToken);
    }
    
    /// <summary>
    /// Unbinds a queue from an exchange.
    /// </summary>
    /// <param name="declaration">The queue declaration.</param>
    /// <param name="binding">The binding declaration containing exchange and routing information.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task QueueUnbindAsync(
        RabbitQueueDeclaration declaration, 
        RabbitQueueBindingDeclaration binding,
        CancellationToken cancellationToken = default)
    {
        await channel.QueueUnbindAsync(
            queue: declaration.Name,
            exchange: binding.ExchangeDeclaration.Name,
            routingKey: binding.RoutingKey ?? declaration.Name,
            arguments: binding.Arguments,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Deletes a queue from the RabbitMQ server.
    /// </summary>
    /// <param name="declaration">The queue declaration to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Handles special cases when UnusedOnly or EmptyOnly flags are set,
    /// catching and logging expected exceptions when queues can't be deleted.
    /// </remarks>
    public async Task QueueDeleteAsync(
        RabbitQueueDeclaration declaration,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await channel.QueueDeleteAsync(
                declaration.Name,
                declaration.UnusedOnly,
                declaration.EmptyOnly,
                declaration.NoWait,
                cancellationToken: cancellationToken);
        }
        catch (OperationInterruptedException) when (declaration.UnusedOnly || declaration.EmptyOnly)
        {
            // RabbitMQ.Client does not ignore PRECONDITION_FAILED
            // Means that queue is used or not empty, so just ignore exception
            // TODO: Informative logging
            logger.LogWarning($"Unable to delete '{declaration.Name}' queue");
        }
    }
    #endregion

    #region Consumers

    /// <summary>
    /// Starts consuming messages from a queue.
    /// </summary>
    /// <param name="serviceProvider">The service provider for dependency injection.</param>
    /// <param name="options">The RabbitMQ options containing serialization configuration.</param>
    /// <param name="queue">The queue declaration to consume from.</param>
    /// <param name="declaration">The consumer declaration containing configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Creates multiple consumers if Count > 1 in the declaration,
    /// appending index numbers to consumer tags for identification.
    /// </remarks>
    public async Task BasicConsumeAsync(
    	IServiceProvider serviceProvider,
    	RabbitOptions options,
    	RabbitQueueDeclaration queue,
    	RabbitConsumerDeclaration declaration,
    	CancellationToken cancellationToken = default)
    {
        for (var index = 0; index < declaration.Count; index++)
        {
            await channel.BasicConsumeAsync(
                queue: queue.Name,
                autoAck: declaration.AutoAck,
                consumerTag: declaration.Tag is null
                    ? string.Empty
                    : $"{declaration.Tag}_{index}",
                noLocal: declaration.NoLocal,
                exclusive: declaration.Exclusive,
                arguments: declaration.Arguments,
                consumer: new RabbitConsumer(channel, options, serviceProvider, queue, declaration, logger),
                cancellationToken: cancellationToken);
        }
    }
    
    /// <summary>
    /// Configures quality of service (QoS) settings for the channel.
    /// </summary>
    /// <param name="declaration">The consumer declaration containing QoS configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Currently only implements prefetchCount (message-based QoS), not prefetchSize (byte-based QoS).
    /// </remarks>
    public async Task BasicQosAsync(
        RabbitConsumerDeclaration declaration, 
        CancellationToken cancellationToken = default)
    {
        // PrefetchSize != 0 is not implemented for now
        await channel.BasicQosAsync(0, declaration.PrefetchCount, declaration.Global, cancellationToken);
    }
    #endregion

    #region Producers

    /// <summary>
    /// Publishes a message to an exchange.
    /// </summary>
    /// <param name="declaration">The producer declaration containing configuration.</param>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task BasicPublishAsync(
        RabbitProducerDeclaration declaration, 
        byte[] payload,
        CancellationToken cancellationToken = default)
    {
        var properties = CreateBasicProperties(declaration);
        await channel.BasicPublishAsync(
            exchange: declaration.ExchangeDeclaration?.Name ?? string.Empty,
            routingKey: declaration.RoutingKey ?? string.Empty,
            mandatory: declaration.Mandatory,
            basicProperties: properties,
            body:payload,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Creates and configures message properties based on the producer declaration.
    /// </summary>
    /// <param name="declaration">The producer declaration containing property configurations.</param>
    /// <returns>A configured BasicProperties instance.</returns>
    private static BasicProperties CreateBasicProperties(RabbitProducerDeclaration declaration)
    {
        var properties = new BasicProperties();
        
        foreach (var property in declaration.Properties)
        {
            property(properties);
        }
    
        return properties;
    }
    #endregion

    #region Transactions

    /// <summary>
    /// Enables transaction mode for the channel.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task TxSelectAsync(CancellationToken cancellationToken = default)
    {
        await channel.TxSelectAsync(cancellationToken);
    }
    
    /// <summary>
    /// Commits the current transaction.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task TxCommitAsync(CancellationToken cancellationToken = default)
    {
        await channel.TxCommitAsync(cancellationToken);
    }
  
    /// <summary>
    /// Rolls back the current transaction.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task TxRollbackAsync(CancellationToken cancellationToken = default)
    {
        await channel.TxRollbackAsync(cancellationToken);
    }
    #endregion

    #region Channel Management

    /// <summary>
    /// Disposes the channel and releases all resources.
    /// </summary>
    public void Dispose()
    {
        channel.Dispose();
    }
    
    /// <summary>
    /// Occurs when a callback exception is thrown.
    /// </summary>
    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync
    {
        add => channel.CallbackExceptionAsync += value;
        remove => channel.CallbackExceptionAsync -= value;
    }
    
    /// <summary>
    /// Occurs when flow control is activated.
    /// </summary>
    public event AsyncEventHandler<FlowControlEventArgs> FlowControlAsync
    {
        add => channel.FlowControlAsync += value;
        remove => channel.FlowControlAsync -= value;
    }
    
    /// <summary>
    /// Occurs when the channel is shutdown.
    /// </summary>
    public event AsyncEventHandler<ShutdownEventArgs> ChannelShutdownAsync
    {
        add => channel.ChannelShutdownAsync += value;
        remove => channel.ChannelShutdownAsync -= value;
    }
    #endregion
}