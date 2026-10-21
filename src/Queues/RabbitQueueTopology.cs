using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Queues;

/// <summary>
/// A hosted service responsible for declaring and managing RabbitMQ queues and their bindings
/// based on the configured <see cref="RabbitOptions"/>.
/// This service ensures that queues are set up or torn down when the application starts or stops.
/// </summary>
internal sealed class RabbitQueueTopology(
    IOptions<RabbitOptions> options,
    IRabbitConnectionProvider connectionProvider)
    : IHostedService
{
    private readonly RabbitOptions _options = options.Value;

    /// <summary>
    /// Starts the hosted service and performs RabbitMQ queue management operations.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that indicates when the start operation should be canceled.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous operation.</returns>
    /// <remarks>
    /// For each queue declaration in the configuration, this method will:
    /// - Delete the queue if marked for deletion
    /// - Or declare the queue and manage its bindings (adding or removing them as configured)
    ///
    /// A retry queue is declared before its main queue, so the main queue's dead letter target already exists.
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var declaration in _options.QueueDeclarations)
        {
            var connection = await connectionProvider.FromDeclarationAsync(declaration.ConnectionDeclaration, cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken);

            if (declaration.Deleted)
            {
                await channel.QueueDeleteAsync(declaration, cancellationToken);

                if (declaration.RetryQueue is not null)
                {
                    await channel.QueueDeleteAsync(declaration.RetryQueue, cancellationToken);
                }

                if (declaration.ParkingQueue is not null)
                {
                    await channel.QueueDeleteAsync(declaration.ParkingQueue, cancellationToken);
                }
            }
            else
            {
                if (declaration.RetryQueue is not null)
                {
                    await channel.QueueDeclareAsync(declaration.RetryQueue, cancellationToken);
                }

                if (declaration.ParkingQueue is not null)
                {
                    await channel.QueueDeclareAsync(declaration.ParkingQueue, cancellationToken);
                }

                await channel.QueueDeclareAsync(declaration, cancellationToken);

                foreach (var binding in declaration.BindingDeclarations)
                {
                    if (binding.Deleted)
                    {
                        await channel.QueueUnbindAsync(declaration, binding, cancellationToken);
                    }
                    else
                    {
                        await channel.QueueBindAsync(declaration, binding, cancellationToken);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Stops the hosted service.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that indicates when the stop operation should be canceled.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous operation.</returns>
    /// <remarks>
    /// This implementation performs no operations during service stop.
    /// </remarks>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
