using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// A hosted service responsible for starting and managing RabbitMQ message consumers
/// based on the configured <see cref="RabbitOptions"/>.
/// </summary>
/// <param name="serviceProvider">The service provider for resolving dependencies.</param>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="connectionProvider">The connection provider for creating RabbitMQ connections.</param>
/// <param name="logger">The logger for shutdown diagnostics.</param>
/// <remarks>
/// This service:
/// - Validates the consumer configuration before touching the broker
/// - Creates consumers for all declared consumer configurations
/// - Tracks every consumer channel and tag it started
/// - Cancels the consumers and closes their channels on graceful shutdown,
///   so in-flight messages settle before the process exits
/// </remarks>
internal sealed class RabbitConsumerHostedService(
    IServiceProvider serviceProvider,
    IOptions<RabbitOptions> options,
    IRabbitConnectionProvider connectionProvider,
    ILogger<RabbitConsumerHostedService> logger)
    : IHostedService
{
    private readonly RabbitOptions _options = options.Value;
    private readonly List<(RabbitChannel Channel, IReadOnlyList<string> ConsumerTags)> _consumers = [];

    /// <summary>
    /// Starts the consumer service and initializes all configured consumers.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// The configuration is validated first, so a contradictory setup fails the application start
    /// with a clear message instead of consuming with silently broken semantics. Then, for each
    /// consumer declaration, a connection and a channel are created, QoS is applied and consuming
    /// starts; the channel and its consumer tags are kept for graceful shutdown.
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Validate();

        foreach (var declaration in _options.ConsumerDeclarations)
        {
            foreach (var queueDeclaration in declaration.QueueDeclarations)
            {
                var connection = await connectionProvider.FromDeclaration(declaration.ConnectionDeclaration, cancellationToken);
                var channel = await connection.CreateChannelAsync(cancellationToken);

                await channel.BasicQosAsync(declaration, cancellationToken);

                var consumerTags = await channel.BasicConsumeAsync(
                    serviceProvider,
                    _options,
                    queueDeclaration,
                    declaration,
                    cancellationToken);

                _consumers.Add((channel, consumerTags));
            }
        }
    }

    /// <summary>
    /// Stops all consumers and closes their channels.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// Each consumer is cancelled before its channel closes, so the broker stops delivering and
    /// unacknowledged messages return to their queues cleanly. A failure on one channel is logged
    /// and does not prevent the remaining channels from being shut down.
    /// </remarks>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var (channel, consumerTags) in _consumers)
        {
            try
            {
                foreach (var consumerTag in consumerTags)
                {
                    await channel.BasicCancelAsync(consumerTag, cancellationToken);
                }

                await channel.DisposeAsync();
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception,
                    "Failed to shut down a consumer channel cleanly; continuing with the remaining channels");
            }
        }

        _consumers.Clear();
    }

    /// <summary>
    /// Rejects consumer configurations whose parts contradict each other.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when a consumer combines settings that cancel each other out.</exception>
    /// <remarks>
    /// AutoAck settles a message before the handler runs, so it can never be requeued or rejected
    /// into a retry queue: combined with Requeue or WithRetry the retry configuration would be
    /// silently dead, which is worse than failing the start.
    /// </remarks>
    private void Validate()
    {
        foreach (var declaration in _options.ConsumerDeclarations)
        {
            if (declaration.AutoAck && declaration.Requeue)
            {
                throw new InvalidOperationException(
                    $"Consumer of '{declaration.PayloadType}' combines AutoAck with Requeue: an auto-acknowledged message can never be requeued");
            }

            if (!declaration.AutoAck)
            {
                continue;
            }

            foreach (var queueDeclaration in declaration.QueueDeclarations)
            {
                if (queueDeclaration.RetryQueue is not null)
                {
                    throw new InvalidOperationException(
                        $"Consumer of '{declaration.PayloadType}' uses AutoAck on queue '{queueDeclaration.Name}' configured with WithRetry: an auto-acknowledged message can never be rejected into the retry queue");
                }
            }
        }
    }
}
