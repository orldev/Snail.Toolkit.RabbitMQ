using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
}