using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
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

        Assert.Equal("exchange", declaration.ExchangeDeclaration?.Name);
        Assert.Equal("connection", declaration.ConnectionDeclaration.Name);
        Assert.False(declaration.Mandatory);
        Assert.Empty(declaration.Properties);
        Assert.Null(declaration.RoutingKey);
        Assert.False(declaration.Transactional);
    }

    /// <remarks>
    /// Per-message key/value data reaches the broker only through the message properties, so the assertion applies
    /// them the way the channel does instead of reading the declaration.
    /// </remarks>
    [Fact]
    public void GetFromSpecified_ReturnTrue()
    {
        var services = new ServiceCollection();
        Action<IBasicProperties> persistent = x => x.Persistent = true;

        services.AddRabbit(o => o
                .UseConnectionUrl("amqp://test.test")
                .UseClientProvidedName("test"),
            c => c
                .AddConnection("exchanges", group =>
                    group.AddProducer<string>(group.AddDirectExchange("exchange"))
                        .Header("x-argument", "argument")
                        .Mandatory()
                        .Property(persistent)
                        .RoutedTo("routing_key")
                        .AppId("app1")
                        .Transactional()));

        var provider = services.BuildServiceProvider();

        var declarations = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ProducerDeclarations;

        var declaration = Assert.Single(declarations.Values);

        var properties = new BasicProperties();
        foreach (var property in declaration.Properties)
        {
            property(properties);
        }

        var (key, value) = Assert.Single(properties.Headers!);
        Assert.Equal("x-argument", key);
        Assert.Equal("argument", value);
        Assert.True(properties.Persistent);
        Assert.Equal("app1", properties.AppId);

        Assert.Equal("exchange", declaration.ExchangeDeclaration?.Name);
        Assert.Equal("exchanges", declaration.ConnectionDeclaration.Name);
        Assert.True(declaration.Mandatory);
        Assert.Equal(3, declaration.Properties.Count);
        Assert.Equal("routing_key", declaration.RoutingKey);
        Assert.True(declaration.Transactional);
    }

    /// <summary>The copy an override works on has the original's settings and actions, in order, in a list of its own.</summary>
    [Fact]
    public void FromDeclaration_CopiesSettingsAndActions_IntoAListOfItsOwn()
    {
        var connection = new Connections.RabbitConnectionDeclaration("main");
        var exchange = new Exchanges.RabbitExchangeDeclaration(connection, "direct", "orders");
        Action<IBasicProperties> persistent = p => p.Persistent = true;
        Action<IBasicProperties> typed = p => p.Type = "order";
        var original = new Producers.RabbitProducerDeclaration(typeof(string), connection, exchange)
        {
            RoutingKey = "orders.created",
            Mandatory = true,
            Transactional = true
        };
        original.AddProperty(persistent);
        original.AddProperty(typed);

        var copy = Producers.RabbitProducerDeclaration.FromDeclaration(original);
        copy.AddProperty(p => p.MessageId = "m-1");

        Assert.Equal((typeof(string), connection, exchange, "orders.created", true, true),
            (copy.PayloadType, copy.ConnectionDeclaration, copy.ExchangeDeclaration, copy.RoutingKey, copy.Mandatory, copy.Transactional));
        Assert.Equal([persistent, typed], copy.Properties.Take(2));
        Assert.Equal(3, copy.Properties.Count);
        Assert.Equal([persistent, typed], original.Properties);
    }
}
