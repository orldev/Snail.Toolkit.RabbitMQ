using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Snail.Toolkit.RabbitMQ.Extensions;
using Snail.Toolkit.RabbitMQ.Queues;
using Snail.Toolkit.RabbitMQ.Queues.Extensions;

namespace Snail.Toolkit.RabbitMQ.Tests.Chaos;

/// <summary>Boundary values and contradictions the queue builder accepts and hands to the broker.</summary>
public sealed class DeclarationChaosTests
{
    private static RabbitQueueDeclaration QueueOf(Func<IRabbitQueueBuilder<string>, IRabbitQueueBuilder<string>> configure)
    {
        var services = new ServiceCollection();
        services.AddRabbit(o => o.UseConnectionUrl("amqp://test.test"),
            cs => cs.AddConnection("connection", c => configure(c.AddQueue<string>("queue"))));

        return Assert.Single(services.BuildServiceProvider().GetRequiredService<IOptions<RabbitOptions>>().Value.QueueDeclarations);
    }

    /// <summary>
    /// The broker reads a TTL as 32-bit milliseconds, and a double-to-int cast saturates: a delay of thirty days once
    /// became a silent retry after 24.85 days. The builder refuses what it cannot keep, naming the parameter.
    /// </summary>
    [Fact]
    public void WithRetry_DelayOfThirtyDays_IsRefusedByName()
    {
        var exception = Record.Exception(() => QueueOf(q => q.WithRetry(TimeSpan.FromDays(30))));

        Assert.Equal("delay", Assert.IsType<ArgumentOutOfRangeException>(exception).ParamName);
    }

    /// <summary>The same limit on the queue's own message TTL.</summary>
    [Fact]
    public void MessageTimeToLive_OfThirtyDays_IsRefusedByName()
    {
        var exception = Record.Exception(() => QueueOf(q => q.MessageTimeToLive(TimeSpan.FromDays(30))));

        Assert.Equal("timeToLive", Assert.IsType<ArgumentOutOfRangeException>(exception).ParamName);
    }

    /// <summary>
    /// WithRetry points the queue's dead letter exchange at the retry queue, and every dead-lettering reason takes that
    /// road: a message expired by MessageTimeToLive would cycle main → retry → main forever, never counting as an
    /// attempt (only "rejected" deaths count). The builder refuses the two together. (Red before the 2026-10-21 fix.)
    /// </summary>
    [Fact]
    public void WithRetry_CombinedWithMessageTimeToLive_IsRefused()
    {
        var exception = Record.Exception(() => QueueOf(q => q.MessageTimeToLive(TimeSpan.FromSeconds(1)).WithRetry(TimeSpan.FromSeconds(1))));

        Assert.IsType<InvalidOperationException>(exception);
    }

    /// <summary>The same refusal in the other order, and for MaxMessageCount, whose overflow dead-letters too.</summary>
    [Fact]
    public void MaxMessageCount_OnARetryingQueue_IsRefused()
    {
        var exception = Record.Exception(() => QueueOf(q => q.WithRetry(TimeSpan.FromSeconds(1)).MaxMessageCount(10)));

        Assert.IsType<InvalidOperationException>(exception);
    }
}
