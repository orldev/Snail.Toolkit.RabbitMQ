using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ConsumerValidationTests
{
    private static RabbitConsumerLifetime HostedServiceFor(Action<IRabbitBuilder> connections)
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            connections);

        var provider = services.BuildServiceProvider();

        return new RabbitConsumerLifetime(
            provider,
            provider.GetRequiredService<IOptions<RabbitOptions>>(),
            new Mock<IRabbitConnectionProvider>(MockBehavior.Strict).Object,
            NullLogger<RabbitConsumerLifetime>.Instance);
    }

    [Fact]
    public async Task AutoAck_CombinedWithWithRetry_FailsTheStart()
    {
        var hostedService = HostedServiceFor(cs => cs
            .AddConnection("connection", c =>
            {
                var queue = c.AddQueue<string>("queue")
                    .Durable()
                    .WithRetry(TimeSpan.FromSeconds(5));

                c.AddConsumer(queue).AutoAck();
            }));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => hostedService.StartAsync(CancellationToken.None));

        Assert.Contains("WithRetry", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AutoAck_CombinedWithRequeue_FailsTheStart()
    {
        var hostedService = HostedServiceFor(cs => cs
            .AddConnection("connection", c =>
            {
                var queue = c.AddQueue<string>("queue").Durable();

                c.AddConsumer(queue).AutoAck().Requeue();
            }));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => hostedService.StartAsync(CancellationToken.None));

        Assert.Contains("Requeue", exception.Message, StringComparison.Ordinal);
    }

    /// <remarks>
    /// The strict connection provider mock fails the start after validation has passed, proving the configuration
    /// itself was accepted.
    /// </remarks>
    [Fact]
    public async Task ManualAck_WithWithRetry_PassesValidation()
    {
        var hostedService = HostedServiceFor(cs => cs
            .AddConnection("connection", c =>
            {
                var queue = c.AddQueue<string>("queue")
                    .Durable()
                    .WithRetry(TimeSpan.FromSeconds(5));

                c.AddConsumer(queue);
            }));

        await Assert.ThrowsAsync<MockException>(
            () => hostedService.StartAsync(CancellationToken.None));
    }
}
