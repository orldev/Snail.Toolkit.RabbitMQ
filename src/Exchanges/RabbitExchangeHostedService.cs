using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Exchanges;

/// <summary>
/// A hosted service responsible for declaring and managing RabbitMQ exchanges and their bindings
/// based on the configured <see cref="RabbitOptions"/>.
/// </summary>
/// <param name="options">The RabbitMQ configuration options.</param>
/// <param name="connectionProvider">The connection provider for creating RabbitMQ connections.</param>
/// <remarks>
/// This service ensures exchanges are properly set up or torn down when the application starts.
/// It handles both exchange declaration/deletion and binding/unbinding operations.
/// </remarks>
internal sealed class RabbitExchangeHostedService(
    IOptions<RabbitOptions> options,
    IRabbitConnectionProvider connectionProvider)
    : IHostedService
{
    private readonly RabbitOptions _options = options.Value;

    /// <summary>
    /// Starts the hosted service and performs RabbitMQ exchange management operations.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// For each exchange declaration in the configuration, this method will:
    /// - Delete the exchange if marked for deletion
    /// - Or declare the exchange and manage its bindings (adding or removing them as configured)
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var declaration in _options.ExchangeDeclarations)
        {
            var connection = await connectionProvider.FromDeclaration(declaration.ConnectionDeclaration, cancellationToken);
            using var channel = await connection.CreateChannelAsync(cancellationToken);
            
            if (declaration.Deleted)
            {
                await channel.ExchangeDeleteAsync(declaration, cancellationToken);
            }
            else
            {
                await channel.ExchangeDeclareAsync(declaration, cancellationToken);
                
                foreach (var binding in declaration.BindingDeclarations)
                {
                    if (binding.Deleted)
                    {
                        await channel.ExchangeUnbindAsync(declaration, binding, cancellationToken);
                    }
                    else
                    {
                        await channel.ExchangeBindAsync(declaration, binding, cancellationToken);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Stops the hosted service.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// This implementation performs no operations during service stop as connections
    /// and channels are disposed automatically when the service provider is disposed.
    /// </remarks>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}