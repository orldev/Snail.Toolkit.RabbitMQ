using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Exchanges.Extensions;
using Snail.Toolkit.RabbitMQ.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ExchangeDeclarationsTests
{
    [Fact]
    public void GetFromDefault_ReturnFalse()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            c => c
                .AddConnection("connection", 
                    group => group.AddDirectExchange("exchange")));
        
        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ExchangeDeclarations;

        var declaration = Assert.Single(declarations);

        Assert.Empty(declaration.Arguments);
        Assert.False(declaration.AutoDelete);
        Assert.False(declaration.Deleted);
        Assert.False(declaration.Durable);
        Assert.Empty(declaration.BindingDeclarations);
        Assert.Equal("exchange", declaration.Name);
        Assert.Equal(ExchangeType.Direct, declaration.Type);
        Assert.Equal("connection", declaration.ConnectionDeclaration.Name);
        Assert.False(declaration.UnusedOnly);
        Assert.False(declaration.NoWait);
    }

    [Fact]
    public void GetFromSpecified_ReturnTrue()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            c => c
                .AddConnection("exchanges", 
                    group => group.AddDirectExchange("exchange")
                        .Argument("x-argument", "argument")
                        .AutoDelete()
                        .Deleted(true)
                        .Durable()
                        .BoundTo(group.AddDirectExchange("exchange2"), b =>
                            b.RoutedTo("routing_key")
                                .Deleted()
                                .NoWait()
                                .Argument("x-argument", "argument"))
                        .NoWait()));
        
        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ExchangeDeclarations;

        var declaration = declarations.First();

        var (key, value) = Assert.Single(declaration.Arguments);
        Assert.Equal("x-argument", key);
        Assert.Equal("argument", value);

        Assert.True(declaration.AutoDelete);
        Assert.True(declaration.Deleted);
        Assert.True(declaration.Durable);
        var binding = Assert.Single(declaration.BindingDeclarations);
        Assert.Equal("routing_key", binding.RoutingKey);
        Assert.Equal("exchange2", binding.ExchangeDeclaration.Name);
        Assert.True(binding.Deleted);
        Assert.True(binding.NoWait);

        (key, value) = Assert.Single(binding.Arguments);
        Assert.Equal("x-argument", key);
        Assert.Equal("argument", value);

        Assert.Equal("exchange", declaration.Name);
        Assert.Equal(ExchangeType.Direct, declaration.Type);
        Assert.Equal("exchanges", declaration.ConnectionDeclaration.Name);
        Assert.True(declaration.UnusedOnly);
        Assert.True(declaration.NoWait);
    }
}