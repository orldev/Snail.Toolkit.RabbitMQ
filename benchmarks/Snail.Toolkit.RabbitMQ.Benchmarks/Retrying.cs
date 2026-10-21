using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>A delivery whose handler fails on a queue that retries: the attempt read from the x-death header, the reject.</summary>
/// <remarks>
/// The header is what the client delivers after two rejections: one death entry per queue the message passed, names and
/// reasons as UTF-8 byte arrays. Each delivery fails with an exception of its own: one instance thrown again and again
/// would grow its stack trace on every throw.
/// </remarks>
[MemoryDiagnoser]
public class Retrying
{
    private readonly BasicProperties _properties = new()
    {
        Headers = new Dictionary<string, object?>
        {
            ["x-death"] = new List<object> { Death("orders.retry", 2), Death("orders", 2) }
        }
    };

    private RabbitConsumer _consumer = null!;

    private ReadOnlyMemory<byte> _body;

    [GlobalSetup]
    public async Task Setup()
    {
        var connection = new RabbitConnectionDeclaration("main");
        var queue = new RabbitQueueDeclaration(connection, "orders")
        {
            RetryQueue = new RabbitQueueDeclaration(connection, "orders.retry"),
            MaxAttempts = 5
        };
        var declaration = new RabbitConsumerDeclaration(typeof(Order), connection, [queue]);
        declaration.AddSubscription(static (_, _, _) => ValueTask.FromException(new InvalidOperationException("The handler failed.")));

        _consumer = new RabbitConsumer(new FakeChannel(), new RabbitOptions(), new ServiceCollection().BuildServiceProvider(), queue, declaration, NullLogger.Instance);
        _body = JsonSerializer.SerializeToUtf8Bytes(Order.Sample());

        await VerifyAsync(queue, declaration);
    }

    [Benchmark]
    public Task DeliverFailed() => _consumer.HandleBasicDeliverAsync("consumer", 1, true, "orders", "orders.created", _properties, _body);

    /// <summary>
    /// Fails the run unless the third attempt is rejected into the retry queue and the fifth, the last of the budget,
    /// is acknowledged and dropped.
    /// </summary>
    private async Task VerifyAsync(RabbitQueueDeclaration queue, RabbitConsumerDeclaration declaration)
    {
        var settled = new List<string>();
        var channel = new FakeChannel { Settling = (tag, acked, requeued) => settled.Add($"{tag}:{(acked ? "ack" : requeued ? "requeue" : "reject")}") };
        var consumer = new RabbitConsumer(channel, new RabbitOptions(), new ServiceCollection().BuildServiceProvider(), queue, declaration, NullLogger.Instance);
        var exhausted = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { ["x-death"] = new List<object> { Death("orders.retry", 4), Death("orders", 4) } }
        };

        await consumer.HandleBasicDeliverAsync("consumer", 1, true, "orders", "orders.created", _properties, _body);
        await consumer.HandleBasicDeliverAsync("consumer", 2, true, "orders", "orders.created", exhausted, _body);

        if (!settled.SequenceEqual(["1:reject", "2:ack"]))
            throw new InvalidOperationException($"The consumer settled {string.Join(", ", settled)} instead of 1:reject, 2:ack.");
    }

    private static Dictionary<string, object?> Death(string queue, long count) => new()
    {
        ["queue"] = Encoding.UTF8.GetBytes(queue),
        ["reason"] = "rejected"u8.ToArray(),
        ["count"] = count,
        ["exchange"] = Array.Empty<byte>()
    };
}
