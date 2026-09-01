using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ReliabilityDeclarationsTests
{
    [Fact]
    public void WithRetry_ConfiguresQueueAndCompanionRetryQueue()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            cs => cs
                .AddConnection("connection",
                    c => c
                        .AddQueue<string>("queue")
                        .Durable()
                        .WithRetry(TimeSpan.FromSeconds(5), maxAttempts: 4)));

        var provider = services.BuildServiceProvider();

        var declaration = Assert.Single(
            provider.GetRequiredService<IOptions<RabbitOptions>>().Value.QueueDeclarations);

        Assert.Equal(string.Empty, declaration.Arguments["x-dead-letter-exchange"]);
        Assert.Equal("queue.retry", declaration.Arguments["x-dead-letter-routing-key"]);
        Assert.Equal(4, declaration.MaxAttempts);

        var retry = declaration.RetryQueue;
        Assert.NotNull(retry);
        Assert.Equal("queue.retry", retry.Name);
        Assert.Equal(5000, retry.Arguments["x-message-ttl"]);
        Assert.Equal(string.Empty, retry.Arguments["x-dead-letter-exchange"]);
        Assert.Equal("queue", retry.Arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public void WithRetry_ThrowsWhenDeadLetterExchangeAlreadyConfigured()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() =>
            services.AddRabbit(o => o
                    .UseConnectionUrl("amqp://test.test")
                    .UseClientProvidedName("test"),
                cs => cs
                    .AddConnection("connection",
                        c => c
                            .AddQueue<string>("queue")
                            .Argument("x-dead-letter-exchange", "dead-letters")
                            .WithRetry(TimeSpan.FromSeconds(5)))));
    }

    [Fact]
    public void QueueTimeToLive_UsesTotalMilliseconds()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            cs => cs
                .AddConnection("connection",
                    c => c
                        .AddQueue<string>("queue")
                        .TimeToLive(TimeSpan.FromMinutes(1))
                        .MessageTimeToLive(TimeSpan.FromSeconds(30))));

        var provider = services.BuildServiceProvider();

        var declaration = Assert.Single(
            provider.GetRequiredService<IOptions<RabbitOptions>>().Value.QueueDeclarations);

        Assert.Equal(60_000, declaration.Arguments["x-expires"]);
        Assert.Equal(30_000, declaration.Arguments["x-message-ttl"]);
    }

    [Fact]
    public void Producer_MessageIdCorrelationIdAndExpiration_AreAppliedToProperties()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            cs => cs
                .AddConnection("connection",
                    c => c
                        .AddProducer<string>()
                        .MessageId("message-1")
                        .CorrelationId("task-1")
                        .MessageTimeToLive(TimeSpan.FromSeconds(30))));

        var provider = services.BuildServiceProvider();

        var declaration = Assert.Single(
            provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ProducerDeclarations.Values);

        var properties = new BasicProperties();
        foreach (var apply in declaration.Properties)
        {
            apply(properties);
        }

        Assert.Equal("message-1", properties.MessageId);
        Assert.Equal("task-1", properties.CorrelationId);
        Assert.Equal("30000", properties.Expiration);
    }
}
