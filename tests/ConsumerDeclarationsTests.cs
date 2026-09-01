using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ConsumerDeclarationsTests
{
    [Fact]
    public void GetFromDefault_ReturnFalse()
    {
        var services = new ServiceCollection();
			
        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            cs => cs
                .AddConnection("connection", 
                    c => c
                        .AddConsumer(c.AddQueue<string>("queue"))));

        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ConsumerDeclarations;

        var declaration = Assert.Single(declarations);

        Assert.Empty(declaration.Arguments);
        Assert.False(declaration.AutoAck);
        Assert.Equal(1u, declaration.Count);
        Assert.False(declaration.Exclusive);
        Assert.False(declaration.Global);
        Assert.False(declaration.Multiple);
        Assert.False(declaration.NoLocal);
        Assert.Equal(0, declaration.PrefetchCount);
        Assert.Equal("queue", Assert.Single(declaration.QueueDeclarations).Name);
        Assert.Equal("connection", declaration.ConnectionDeclaration.Name);
        Assert.False(declaration.Requeue);
        Assert.Null(declaration.Tag);
    }

    [Fact]
    public void GetFromSpecified_ReturnTrue()
    {
        var services = new ServiceCollection();
			
        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            cs => cs
                .AddConnection("consumers", 
                    c => c
                        .AddConsumer(c.AddQueue<string>("queue"))
                        .Argument("x-argument", "argument")
                        .AutoAck()
                        .Count(2)
                        .Exclusive()
                        .Requeue(true)
                        .NoLocal()
                        .Prefetch(2)
                        .Tagged("tag")));

        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ConsumerDeclarations;

        var declaration = Assert.Single(declarations);

        var (key, value) = Assert.Single(declaration.Arguments);
        Assert.Equal("x-argument", key);
        Assert.Equal("argument", value);

        Assert.True(declaration.AutoAck);
        Assert.Equal(2u, declaration.Count);
        Assert.True(declaration.Exclusive);
        Assert.False(declaration.Global);
        Assert.Equal("consumers", declaration.ConnectionDeclaration.Name);
        Assert.True(declaration.Multiple);
        Assert.True(declaration.NoLocal);
        Assert.Equal(2, declaration.PrefetchCount);
        Assert.Equal("queue", Assert.Single(declaration.QueueDeclarations).Name);
        Assert.True(declaration.Requeue);
        Assert.Equal("tag", declaration.Tag);
    }
}