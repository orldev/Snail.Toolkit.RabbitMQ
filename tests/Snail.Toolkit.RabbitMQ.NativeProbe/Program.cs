using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.NativeProbe;
using Snail.Toolkit.RabbitMQ.Producers;
using Snail.Toolkit.RabbitMQ.Producers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues;

var configuration = new ConfigurationBuilder()
    .AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["RabbitMQ:HostName"] = "broker.internal",
        ["RabbitMQ:Port"] = "5671",
        ["RabbitMQ:UserName"] = "probe"
    })
    .Build();

var connection = new RabbitConnectionDeclaration("main");
var fake = new FakeChannel();
var channel = new RabbitChannel(connection, fake, NullLogger<RabbitChannel>.Instance);
var services = new ServiceCollection();

services.AddLogging();
services.AddRabbit(configuration, rabbit => rabbit.AddConnection("main", main => main.AddProducer<Order>().RoutedTo("orders")));
services.Configure<RabbitOptions>(options => options.UseSerialization(
    payload => JsonSerializer.SerializeToUtf8Bytes(payload, ProbeJson.Default.GetTypeInfo(payload.GetType())!),
    (body, type) => JsonSerializer.Deserialize(body, ProbeJson.Default.GetTypeInfo(type)!)!));
services.AddSingleton<IRabbitChannelProvider>(new OpenChannel(channel));

await using var provider = services.BuildServiceProvider();
var sent = new Order("order-1", 3, 49.30m);

await provider.GetRequiredService<IRabbitProducer>().PublishAsync(sent);

var queue = new RabbitQueueDeclaration(connection, "orders");
var consumer = new RabbitConsumerDeclaration(typeof(Order), connection, [queue]);
Order? received = null;
consumer.AddSubscription((_, payload, _) =>
{
    received = (Order)payload;
    return ValueTask.CompletedTask;
});

await channel.BasicConsumeAsync(provider, provider.GetRequiredService<IOptions<RabbitOptions>>().Value, queue, consumer);
await fake.DeliverAsync();

var isDelivered = received == sent && fake.Acknowledged == 1;

Console.WriteLine($"published {fake.Body.Length} bytes, received {received}, acknowledged {fake.Acknowledged}");

return isDelivered ? 0 : 1;
