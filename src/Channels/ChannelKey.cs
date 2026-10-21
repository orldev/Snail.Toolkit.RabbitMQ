namespace Snail.Toolkit.RabbitMQ.Channels;

/// <summary>
/// Identifies a cached producer channel by the only state a channel actually carries.
/// </summary>
/// <param name="Connection">The name of the connection the channel lives on.</param>
/// <param name="IsTransactional">Whether the channel runs in transaction mode.</param>
/// <remarks>
/// A channel is not bound to an exchange or a routing key — both are supplied per publish —
/// so keying by them would only multiply channels and eventually exhaust the broker's
/// per-connection channel limit. What a channel does carry is its connection and its mode:
/// a transactional channel ran TxSelect, a regular one tracks publisher confirmations,
/// and the two modes are mutually exclusive on one channel.
/// </remarks>
internal readonly record struct ChannelKey(string Connection, bool IsTransactional);
