using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;

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
    /// A <see cref="ValueTask"/> that completes once the broker accepts the message,
    /// and throws when publishing fails.
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
    /// when the broker rejects it. Transactional publishes are serialized per channel,
    /// because an AMQP transaction is channel-wide state that concurrent publishes would corrupt.
    /// </remarks>
    ValueTask PublishAsync<T>(
        [DisallowNull] T payload,
        Action<IRabbitProducerBuilder<T>>? overrides = null,
        CancellationToken cancellationToken = default);

}

/// <summary>
/// Default implementation of <see cref="IRabbitProducer"/> that publishes messages to RabbitMQ.
/// </summary>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="channelProvider">The channel provider for obtaining RabbitMQ channels.</param>
/// <param name="connectionProvider">The connection provider, whose connection a publish waits on while the client recovers it.</param>
/// <param name="logger">The logger for producer diagnostics.</param>
/// <remarks>
/// This implementation handles:
/// - Message serialization using the configured serializer
/// - Transaction management (commit/rollback), serialized per channel
/// - Channel lifecycle management
/// - Configuration overrides
/// </remarks>
internal sealed class RabbitProducer(
    IOptions<RabbitOptions> options,
    IRabbitChannelProvider channelProvider,
    IRabbitConnectionProvider connectionProvider,
    ILogger<RabbitProducer> logger)
    : IRabbitProducer
{
    private readonly RabbitOptions _options = options.Value;

    /// <inheritdoc/>
    /// <remarks>
    /// <para>
    /// A publish whose confirm is still to come suspends here; its state machine comes from a pool instead of a new
    /// object per message, so the returned task, like any <see cref="ValueTask"/>, is awaited once. The message
    /// properties are kept per thread as well and cleared for the next publish once this one completed.
    /// </para>
    /// <para>
    /// A publish that finds its connection lost — the socket gone, or the broker closing the connection — waits for
    /// the client's automatic recovery, as long as the options allow, and is then published once more on a fresh
    /// channel. A message the broker received just before the connection dropped may so arrive twice: delivery stays
    /// at least once, and a message id lets the consumer tell the copies apart.
    /// </para>
    /// </remarks>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishAsync<T>(
        [DisallowNull] T payload,
        Action<IRabbitProducerBuilder<T>>? overrides = null,
        CancellationToken cancellationToken = default)
    {
        var declaration = GetDeclaration(overrides);
        using var body = _options.Serialize(payload);

        try
        {
            await PublishOnceAsync(declaration, body.Memory, cancellationToken);
        }
        catch (AlreadyClosedException lost) when (CanWaitForRecovery(lost))
        {
            Log.PublishWaitingForRecovery(logger, lost, declaration.ConnectionDeclaration.Name, _options.RecoveryWait);
            var connection = await connectionProvider.FromDeclarationAsync(declaration.ConnectionDeclaration, cancellationToken);
            var isRecovered = await connection.HasRecoveredAsync(_options.RecoveryWait, cancellationToken);

            if (!isRecovered)
            {
                throw;
            }

            await PublishOnceAsync(declaration, body.Memory, cancellationToken);
        }
    }

    /// <summary>
    /// Publishes the body once on the declaration's cached channel, inside a transaction when the declaration asks.
    /// </summary>
    /// <param name="declaration">The producer declaration to publish with.</param>
    /// <param name="body">The serialized message.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that completes once the broker accepted (or confirmed) the message.</returns>
    /// <remarks>
    /// Its state machine is pooled like the caller's, so a publish whose confirm comes later still allocates nothing
    /// for the second one.
    /// </remarks>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PublishOnceAsync(
        RabbitProducerDeclaration declaration,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        var channel = await channelProvider.FromDeclarationAsync(declaration, cancellationToken);
        using var properties = PublishProperties.Rent();

        if (!declaration.Transactional)
        {
            await channel.PublishAsync(declaration, properties, body, cancellationToken);
            return;
        }

        await channel.PublishLock.WaitAsync(cancellationToken);
        try
        {
            await channel.PublishAsync(declaration, properties, body, cancellationToken);
            await channel.TxCommitAsync(cancellationToken);
        }
        catch
        {
            await TryRollbackAsync(channel, cancellationToken);
            throw;
        }
        finally
        {
            channel.PublishLock.Release();
        }
    }

    /// <summary>
    /// Whether a failed publish is worth waiting on: the connection was lost, the client recovers it, and the
    /// options give the recovery time.
    /// </summary>
    /// <param name="exception">The failure of the first publish.</param>
    /// <returns><c>true</c> when the publish should wait for the connection's recovery and try once more.</returns>
    /// <remarks>
    /// A connection is lost when the client itself saw it go (a dropped socket, a missed heartbeat) or the broker
    /// closed it (320, CONNECTION_FORCED: a restart, an operator). A channel the broker closed for a protocol error
    /// (404, 406…) is not: publishing the same message again would fail again, so that failure surfaces at once.
    /// </remarks>
    private bool CanWaitForRecovery(AlreadyClosedException exception)
    {
        if (_options.RecoveryWait <= TimeSpan.Zero || !_options.ConnectionFactory.AutomaticRecoveryEnabled)
        {
            return false;
        }

        return exception.ShutdownReason is { } reason
            && (reason.Initiator == ShutdownInitiator.Library
                || (reason.Initiator == ShutdownInitiator.Peer && reason.ReplyCode == Constants.ConnectionForced));
    }

    /// <summary>
    /// Rolls back the channel's transaction without masking the publish failure being handled.
    /// </summary>
    /// <param name="channel">The transactional channel to roll back.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <remarks>
    /// A publish often fails because the channel itself just died, in which case the rollback
    /// fails too; that secondary failure is logged so the caller sees the original exception.
    /// </remarks>
    private async Task TryRollbackAsync(RabbitChannel channel, CancellationToken cancellationToken)
    {
        try
        {
            await channel.TxRollbackAsync(cancellationToken);
        }
        catch (Exception exception)
        {
            Log.RollbackFailed(logger, exception, channel.ConnectionDeclaration.Name);
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
