using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Connections;
using Snail.Toolkit.RabbitMQ.Consumers;
using Snail.Toolkit.RabbitMQ.Consumers.Extensions;
using Snail.Toolkit.RabbitMQ.Queues;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>One delivery through the consumer: the body read into the message, a scope, the handler, the ack.</summary>
/// <remarks>
/// With <see cref="IsHandledLater"/> the handler's work is still running when the delivery call returns and completes
/// right after, as a handler that awaits a database does, so the awaiting code builds what it builds for real work.
/// </remarks>
[MemoryDiagnoser]
public class Consuming
{
    private readonly BasicProperties _properties = new();

    private readonly Work _work = new();

    private RabbitConsumer _consumer = null!;

    private ReadOnlyMemory<byte> _body;

    /// <summary>Whether the handler's work completes after the delivery call returned, instead of at once.</summary>
    [Params(false, true)]
    public bool IsHandledLater { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connection = new RabbitConnectionDeclaration("main");
        var queue = new RabbitQueueDeclaration(connection, "orders");
        var declaration = new RabbitConsumerDeclaration(typeof(Order), connection, [queue]);
        var services = new ServiceCollection();
        _work.IsLater = IsHandledLater;
        services.AddSingleton(_work);
        new RabbitConsumerBuilder<Order>(services, declaration).Subscribe<Order, OrderHandler>();

        _consumer = new RabbitConsumer(new FakeChannel(), new RabbitOptions(), services.BuildServiceProvider(), queue, declaration, NullLogger.Instance);
        _body = JsonSerializer.SerializeToUtf8Bytes(Order.Sample());

        await VerifyAsync(connection, queue);
    }

    [Benchmark]
    public Task Deliver()
    {
        var delivering = _consumer.HandleBasicDeliverAsync("consumer", 1, false, "orders", "orders.created", _properties, _body);
        _work.Finish();

        return delivering;
    }

    /// <summary>Fails the run unless the delivered body reaches a handler as the order it was written from, and is acknowledged once.</summary>
    private async Task VerifyAsync(RabbitConnectionDeclaration connection, RabbitQueueDeclaration queue)
    {
        Order? received = null;
        var declaration = new RabbitConsumerDeclaration(typeof(Order), connection, [queue]);
        var services = new ServiceCollection();
        new RabbitConsumerBuilder<Order>(services, declaration).Subscribe<Order>(order =>
        {
            received = order;
            return ValueTask.CompletedTask;
        });
        var settled = new List<string>();
        var channel = new FakeChannel { Settling = (tag, acked, requeued) => settled.Add($"{tag}:{(acked ? "ack" : requeued ? "requeue" : "reject")}") };
        var consumer = new RabbitConsumer(channel, new RabbitOptions(), services.BuildServiceProvider(), queue, declaration, NullLogger.Instance);

        await consumer.HandleBasicDeliverAsync("consumer", 1, false, "orders", "orders.created", _properties, _body);

        if (received is null || !JsonSerializer.SerializeToUtf8Bytes(received).AsSpan().SequenceEqual(_body.Span))
            throw new InvalidOperationException("The consumer handed the handler another order than the one delivered.");

        if (!settled.SequenceEqual(["1:ack"]))
            throw new InvalidOperationException($"The consumer settled {string.Join(", ", settled)} instead of 1:ack.");
    }

    /// <summary>A handler that reads the message and does nothing else, so the delivery is the library's work alone.</summary>
    public sealed class OrderHandler(Work work) : IConsumer<Order>
    {
        public ValueTask HandleAsync(Order message, CancellationToken cancellationToken = default)
        {
            if (message.Lines.Count == 0)
                return ValueTask.FromException(new InvalidOperationException("An order without lines."));

            return work.IsLater ? work.Pending() : ValueTask.CompletedTask;
        }
    }

    /// <summary>The handler's work: done at once, or still running until the benchmark finishes it.</summary>
    public sealed class Work
    {
        private readonly PendingConfirm _pending = new();

        /// <summary>Gets or sets whether the work completes after the delivery call returned.</summary>
        public bool IsLater { get; set; }

        internal ValueTask Pending() => _pending.Next();

        /// <summary>Completes the work the handler started, if it is still running.</summary>
        public void Finish() => _pending.Arrive();
    }
}
