using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Exchanges.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ProducerDeclarationsTests
{
    [Fact]
    public void GetFromDefault_ReturnFalse()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            c => c
                .AddConnection("connection", group => 
                    group.AddProducer<string>(group.AddDirectExchange("exchange"))));
			
        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ProducerDeclarations;

        var declaration = Assert.Single(declarations.Values);

        Assert.Empty(declaration.Arguments);
			
        Assert.Equal("exchange", declaration.ExchangeDeclaration?.Name);
        Assert.Equal("connection", declaration.ConnectionDeclaration.Name);
        Assert.False(declaration.Mandatory);
        Assert.Empty(declaration.Properties);
        Assert.Null(declaration.RoutingKey);
        Assert.False(declaration.Transactional);
    }

    [Fact]
    public void GetFromSpecified_ReturnTrue()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            c => c
                .AddConnection("exchanges", group => 
                    group.AddProducer<string>(group.AddDirectExchange("exchange"))
                        .Argument("x-argument", "argument")
                        .Mandatory()
                        .Property(x => x.Persistent = true)
                        .RoutedTo("routing_key")
                        .AppId("app1")
                        .Transactional()));
			
        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ProducerDeclarations;

        var declaration = Assert.Single(declarations.Values);

        var (key, value) = Assert.Single(declaration.Arguments);
        Assert.Equal("x-argument", key);
        Assert.Equal("argument", value);
			
        Assert.Equal("exchange", declaration.ExchangeDeclaration?.Name);
        Assert.Equal("exchanges", declaration.ConnectionDeclaration.Name);
        Assert.True(declaration.Mandatory);
        Assert.Equal(2, declaration.Properties.Count);
        Assert.Equal("routing_key", declaration.RoutingKey);
        Assert.True(declaration.Transactional);
    }
}