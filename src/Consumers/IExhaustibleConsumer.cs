namespace Snail.Toolkit.RabbitMQ.Consumers;

/// <summary>
/// A consumer that is told when the broker gives one of its messages up for good.
/// </summary>
/// <typeparam name="T">The type of message this consumer handles.</typeparam>
/// <remarks>
/// A consumer that throws hands its message back for another attempt, and it cannot tell its last attempt from the
/// others — so a message whose attempts ran out was parked or dropped with nobody upstream told. This is called once
/// a message will not be delivered to the consumer again: its retry attempts exhausted, rejected without a requeue, or
/// lost to automatic acknowledgement. It runs before the message is settled, in a scope of its own, and a failure in it
/// is logged and settles the message all the same. It is not called for a delivery a stopping host returns to the
/// queue, nor for a message that could not be deserialized, since no consumer ever saw that one. A message whose
/// parking fails goes round the retry cycle once more, and is given up — and told — again.
/// </remarks>
public interface IExhaustibleConsumer<in T> : IConsumer<T>
{
    /// <summary>
    /// Learns that a message will not be delivered again, and the failure of its last attempt.
    /// </summary>
    /// <param name="message">The message given up.</param>
    /// <param name="failure">What its last attempt threw.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A ValueTask that represents the asynchronous operation.</returns>
    ValueTask ExhaustedAsync(T message, Exception failure, CancellationToken cancellationToken = default);
}
