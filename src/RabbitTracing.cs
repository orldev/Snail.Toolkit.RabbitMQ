using System.Diagnostics;
using RabbitMQ.Client;

namespace Snail.Toolkit.RabbitMQ;

/// <summary>
/// Exposes the distributed tracing this library takes part in.
/// </summary>
/// <remarks>
/// The spans themselves come from <c>RabbitMQ.Client</c>, which already emits a producer span per
/// publish, a consumer span per delivery, and carries the trace context between them in the message
/// headers. This library adds no span of its own — a second one over the same work would only
/// duplicate it — and instead records on the delivery span the one thing the client cannot know:
/// how the message was settled.
/// </remarks>
/// <example>
/// <code>
/// services.AddOpenTelemetry().WithTracing(tracing => tracing.AddSource(RabbitTracing.SourceNames));
/// </code>
/// </example>
public static class RabbitTracing
{
    /// <summary>
    /// Gets the activity source names to subscribe to in order to collect RabbitMQ traces.
    /// </summary>
    /// <returns>A fresh array, so a caller cannot alter what the next one sees.</returns>
    public static string[] SourceNames =>
    [
        RabbitMQActivitySource.PublisherSourceName,
        RabbitMQActivitySource.SubscriberSourceName
    ];

    /// <summary>
    /// The tag recording how a delivery was settled; see <see cref="Settlement"/> for the values.
    /// </summary>
    public const string SettlementTag = "snail.messaging.settlement";

    /// <summary>
    /// The tag recording the 1-based processing attempt of a delivery that is being retried.
    /// </summary>
    public const string AttemptTag = "snail.messaging.attempt";

    /// <summary>
    /// The tag marking a delivery whose payload could not be deserialized.
    /// </summary>
    public const string PoisonTag = "snail.messaging.poison";

    /// <summary>
    /// The values <see cref="SettlementTag"/> takes.
    /// </summary>
    public static class Settlement
    {
        /// <summary>The handler succeeded and the message was acknowledged.</summary>
        public const string Acked = "acked";

        /// <summary>The message was rejected back onto its queue for one immediate redelivery.</summary>
        public const string Requeued = "requeued";

        /// <summary>The message was rejected without requeue, dead-lettering it when the queue has an exchange for that.</summary>
        public const string Rejected = "rejected";

        /// <summary>The message was rejected into the companion retry queue and will come back after the delay.</summary>
        public const string RetryScheduled = "retry_scheduled";

        /// <summary>The message was copied into the parking queue and acknowledged.</summary>
        public const string Parked = "parked";

        /// <summary>The message was acknowledged and discarded, because no parking queue is configured.</summary>
        public const string Dropped = "dropped";

        /// <summary>The handler failed on an auto-acknowledged message, which the broker had already settled.</summary>
        public const string Lost = "lost";
    }

    /// <summary>
    /// Records on the current delivery span how the message was settled.
    /// </summary>
    /// <param name="settlement">One of the <see cref="Settlement"/> values.</param>
    /// <remarks>
    /// Does nothing when nothing is listening: without a collector the client starts no activity,
    /// <see cref="Activity.Current"/> is null and tracing costs a null check per delivery.
    /// </remarks>
    internal static void Settled(string settlement)
    {
        Activity.Current?.SetTag(SettlementTag, settlement);
    }

    /// <summary>
    /// Records on the current delivery span the failure that decided the settlement.
    /// </summary>
    /// <param name="exception">The failure, or null when the payload merely deserialized to null.</param>
    internal static void Failed(Exception? exception)
    {
        if (Activity.Current is not { } activity)
        {
            return;
        }

        if (exception is not null)
        {
            activity.AddException(exception);
        }

        activity.SetStatus(ActivityStatusCode.Error, exception?.Message);
    }

    /// <summary>
    /// Marks the current delivery span as carrying a payload that could not be deserialized.
    /// </summary>
    internal static void Poison()
    {
        Activity.Current?.SetTag(PoisonTag, true);
    }

    /// <summary>
    /// Records the processing attempt a delivery is on.
    /// </summary>
    /// <param name="attempt">The 1-based attempt number read from the message's x-death header.</param>
    internal static void Attempt(int attempt)
    {
        Activity.Current?.SetTag(AttemptTag, attempt);
    }
}
