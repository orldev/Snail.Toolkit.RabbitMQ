using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void GetConnectionFactory_ReturnEqual()
    {
        var services = new ServiceCollection();

        services.AddRabbit(o =>
        {
            o.ConnectionFactory.ClientProvidedName = "test";
            o.ConnectionFactory.HostName = "test.test";
            o.ConnectionFactory.UserName = "test";
            o.ConnectionFactory.Password = "password";
        });

        var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<RabbitOptions>>().Value;

        Assert.Equal("test", options.ConnectionFactory.ClientProvidedName);
        Assert.Equal("test.test", options.ConnectionFactory.HostName);
        Assert.Equal("test", options.ConnectionFactory.UserName);
        Assert.Equal("password", options.ConnectionFactory.Password);
    }

    /// <remarks>An unset TLS server name must not stay empty: certificate validation would fail on it.</remarks>
    [Fact]
    public void AddRabbitFromConfiguration_BindsTheWholeEndpoint()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMQ:HostName"] = "broker.internal",
                ["RabbitMQ:Port"] = "5671",
                ["RabbitMQ:VirtualHost"] = "orders",
                ["RabbitMQ:UserName"] = "app",
                ["RabbitMQ:Password"] = "secret",
                ["RabbitMQ:ClientProvidedName"] = "orders-service",
                ["RabbitMQ:Ssl:Enabled"] = "true"
            })
            .Build();

        var services = new ServiceCollection();

        services.AddRabbit(configuration, _ => { });

        var provider = services.BuildServiceProvider();

        var factory = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ConnectionFactory;

        Assert.Equal("broker.internal", factory.HostName);
        Assert.Equal(5671, factory.Port);
        Assert.Equal("orders", factory.VirtualHost);
        Assert.Equal("app", factory.UserName);
        Assert.Equal("secret", factory.Password);
        Assert.Equal("orders-service", factory.ClientProvidedName);
        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("broker.internal", factory.Ssl.ServerName);
    }

    [Fact]
    public void AddRabbitFromConfiguration_WithoutSettings_KeepsClientDefaults()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMQ:HostName"] = "broker.internal"
            })
            .Build();

        var services = new ServiceCollection();

        services.AddRabbit(configuration, _ => { });

        var provider = services.BuildServiceProvider();

        var factory = provider.GetRequiredService<IOptions<RabbitOptions>>().Value.ConnectionFactory;

        Assert.Equal(AmqpTcpEndpoint.UseDefaultPort, factory.Port);
        Assert.Equal("/", factory.VirtualHost);
        Assert.False(factory.Ssl.Enabled);
    }
}
