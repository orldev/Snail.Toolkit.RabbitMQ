using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Channels;

namespace Snail.Toolkit.RabbitMQ.Producers;

/// <summary>
/// Represents a RabbitMQ message producer capable of publishing messages to exchanges.
/// </summary>
public interface IRabbitProducer
{
    /// <summary>
    /// Publishes a message to RabbitMQ with optional configuration overrides.
    /// </summary>
    /// <typeparam name="T">The type of the message payload.</typeparam>
    /// <param name="payload">The message payload to publish (cannot be null).</param>
    /// <param name="overrides">Optional action to override default producer configuration.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that completes with true if the message was published successfully,
    /// or throws an exception if publishing fails.
    /// </returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="payload"/> is null.</exception>
    /// <exception cref="RabbitMissingDeclarationException">
    /// Thrown if no producer declaration is found for type <typeparamref name="T"/>.
    /// </exception>
    /// <remarks>
    /// This method will:
    /// 1. Look up the default configuration for message type <typeparamref name="T"/>
    /// 2. Apply any configuration overrides if provided
    /// 3. Publish the message through a channel from the pool
    /// 4. Handle transactions if configured (commit on success, rollback on failure)
    ///
    /// Non-transactional producers publish on channels with publisher confirmations enabled:
    /// the returned task completes only after the broker confirms the message and throws
    /// when the broker rejects it.
    /// </remarks>
    ValueTask<bool> PublishAsync<T>(
        [DisallowNull] T payload,
        Action<IRabbitProducerBuilder<T>>? overrides = null,
        CancellationToken cancellationToken = default);
    
}

/// <summary>
/// Default implementation of <see cref="IRabbitProducer"/> that publishes messages to RabbitMQ.
/// </summary>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="channelProvider">The channel provider for obtaining RabbitMQ channels.</param>
/// <remarks>
/// This implementation handles:
/// - Message serialization using the configured serializer
/// - Transaction management (commit/rollback)
/// - Channel lifecycle management
/// - Configuration overrides
/// </remarks>
internal sealed class RabbitProducer(
    IOptions<RabbitOptions> options,
    IRabbitChannelProvider channelProvider)
    : IRabbitProducer
{
    private readonly RabbitOptions _options = options.Value;

    /// <inheritdoc/>
    public async ValueTask<bool> PublishAsync<T>(
        [DisallowNull] T payload,
        Action<IRabbitProducerBuilder<T>>? overrides = null,
        CancellationToken cancellationToken = default)
    {
        var declaration = GetDeclaration(overrides);
        var channel = await channelProvider.FromDeclaration(declaration, cancellationToken);
        
        try
        {
            await channel.BasicPublishAsync(declaration, _options.Serializer(payload), cancellationToken);

            if (declaration.Transactional)
            {
                await channel.TxCommitAsync(cancellationToken);
            }
            
            return true;
        }
        catch
        {
            if (declaration.Transactional)
            {
                await channel.TxRollbackAsync(cancellationToken);
            }

            throw;
        }
    }
    
    /// <summary>
    /// Gets the producer declaration, applying any configuration overrides.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="overrides">Optional action to override default configuration.</param>
    /// <returns>The producer declaration to use.</returns>
    /// <exception cref="RabbitMissingDeclarationException">
    /// Thrown if no producer declaration is registered for type <typeparamref name="T"/>.
    /// </exception>
    private RabbitProducerDeclaration GetDeclaration<T>(Action<IRabbitProducerBuilder<T>>? overrides)
    {
        var producerDeclaration = _options.ProducerDeclarations.TryGetValue(typeof(T), out var declaration)
            ? declaration
            : throw new RabbitMissingDeclarationException(typeof(T));

        if (overrides is not null)
        {
            producerDeclaration = RabbitProducerDeclaration.FromDeclaration(producerDeclaration);
            overrides(new RabbitProducerBuilder<T>(producerDeclaration));
        }

        return producerDeclaration;
    }
}