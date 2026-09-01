using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// A hosted service responsible for starting and managing RabbitMQ message consumers
/// based on the configured <see cref="RabbitOptions"/>.
/// </summary>
/// <param name="serviceProvider">The service provider for resolving dependencies.</param>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="connectionProvider">The connection provider for creating RabbitMQ connections.</param>
/// <remarks>
/// This service:
/// - Starts during application initialization
/// - Creates consumers for all declared consumer configurations
/// - Manages the lifecycle of consumer channels
/// - Handles graceful shutdown (though current implementation is minimal)
/// </remarks>
internal sealed class RabbitConsumerHostedService(
    IServiceProvider serviceProvider,
    IOptions<RabbitOptions> options,
    IRabbitConnectionProvider connectionProvider)
    : IHostedService
{
    private readonly RabbitOptions _options = options.Value;

    /// <summary>
    /// Starts the consumer service and initializes all configured consumers.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// For each consumer declaration in the configuration, this method will:
    /// 1. Create a connection using the specified connection declaration
    /// 2. Create a channel for consuming messages
    /// 3. Configure QoS (Quality of Service) settings
    /// 4. Start consuming messages from each declared queue
    /// 
    /// Note: Current implementation creates multiple channels per connection which may need optimization.
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var declaration in _options.ConsumerDeclarations)
        {
            foreach (var queueDeclaration in declaration.QueueDeclarations)
            {
                var connection = await connectionProvider.FromDeclaration(declaration.ConnectionDeclaration, cancellationToken);
                // TODO: fix 2 channel - currently creates multiple channels per connection
                var channel = await connection.CreateChannelAsync(cancellationToken);
                
                await channel.BasicQosAsync(declaration, cancellationToken);
                
                await channel.BasicConsumeAsync(
                    serviceProvider,
                    _options,
                    queueDeclaration,
                    declaration,
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// Stops the consumer service.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// Current implementation does not perform any active shutdown operations.
    /// Channels and connections will be disposed automatically when the service provider is disposed.
    /// </remarks>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}