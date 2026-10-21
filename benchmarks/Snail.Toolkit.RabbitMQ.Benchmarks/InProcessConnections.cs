using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Snail.Toolkit.RabbitMQ.Channels;
using Snail.Toolkit.RabbitMQ.Connections;

namespace Snail.Toolkit.RabbitMQ.Benchmarks;

/// <summary>Connections that open the fake channel, so the library's own channel provider can be timed without a broker.</summary>
internal sealed class InProcessConnections(IChannel channel) : IRabbitConnectionProvider
{
    public Task<RabbitConnection> FromDeclarationAsync(RabbitConnectionDeclaration connection, CancellationToken cancellationToken = default)
    {
        var client = DispatchProxy.Create<IConnection, ConnectionProxy>();
        ((ConnectionProxy)(object)client).Channel = channel;

        return Task.FromResult(new RabbitConnection(NullLogger<RabbitChannel>.Instance, connection, client));
    }
}

/// <summary>An AMQP connection that only opens channels: every channel it opens is the one it was given.</summary>
/// <remarks>Left open: <see cref="DispatchProxy"/> derives the proxy from it at run time.</remarks>
public class ConnectionProxy : DispatchProxy
{
    internal IChannel Channel { get; set; } = null!;

    /// <summary>Opens the channel as the options say: a fake channel learns whether it tracks publisher confirmations.</summary>
    private Task<IChannel> Open(CreateChannelOptions? options)
    {
        if (Channel is FakeChannel fake)
            fake.TracksConfirmations = options?.PublisherConfirmationsEnabled ?? false;

        return Task.FromResult(Channel);
    }

    /// <inheritdoc />
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
        targetMethod?.Name switch
        {
            nameof(IConnection.CreateChannelAsync) => Open(args?[0] as CreateChannelOptions),
            _ => throw new NotSupportedException($"The in-process connection does not answer {targetMethod?.Name}.")
        };
}
