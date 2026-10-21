# Snail.Toolkit.RabbitMQ

Extension for the framework `RabbitMQ.Client`

## Concepts
- **Release immutability**
    - The topology for each release must be strictly defined and not changed during its existence
        - Until application is running you can't:
            - Cancel consumers
            - Remove queues and exchanges
            - Change bindings

- **Declarativeness and simplicity**
    - Intuitive RabbitMQ-close fluent interfaces
    - Generic type checks
    - Connection, Exchange, Queue, Consumer and Producer declaration classes

- **Reliability by default**
    - Publisher confirmations: `PublishAsync` completes only after the broker confirms the message
      and throws when it is rejected or unroutable (`Mandatory`)
    - Poison messages (payload that cannot be deserialized) never stall a consumer and never
      cycle through the retry queue — they are parked, dropped or rejected depending on the queue setup
    - Retry with backoff via a companion retry queue: `WithRetry` on a queue, with optional
      parking of exhausted messages (`parkTo`) for manual inspection
    - A dead producer channel is evicted and replaced automatically — one broker-side channel
      error never disables publishing until restart
    - A publish that finds its connection lost waits for the client to recover it (30 s unless
      `UseRecoveryWait` says otherwise) and is then published once more, instead of failing
    - Contradictory consumer configuration (`AutoAck` + `WithRetry`/`Requeue`) fails the
      application start with a clear message instead of silently not retrying
    - Graceful shutdown: consumers are cancelled and channels and connections closed when the host stops
    - Traces out of the box: publish and delivery share one trace, and the delivery span records
      how the message was settled (see [Tracing](#tracing))

## Installation

```bash
dotnet add package Snail.Toolkit.RabbitMQ
```

## Usage

```csharp
services.AddRabbit(options =>
        options.UseConnectionFactory(factory => ...), 
    connections => connections
    .AddConnection(connection =>
    {
        // Enable type checks with .AddDirectExchange<TPayload>() extension
        var exchange = connection.AddDirectExchange("exchange")
            // .NoWait()
            // .Deleted()
            .AutoDelete()
            .Durable();

        var queue = connection.AddQueue<Payload>("queue")
            // .Exclusive()
            // .Deleted()
            // .NoWait()
            // .Lazy()
            // .MaxPriority(10)
            // .TimeToLive(10000)
            // .MaxMessageSize(1000)
            // .MaxMessageCount(1000)
            // .MessageTimeToLive(1000)
            // .RejectPublish()
            // .Quorum()
            // .SingleActiveConsumer()
            // Failed messages wait in "queue.retry" for 30s, then are redelivered;
            // after 5 attempts the message moves to "queue.failed" (or is dropped without parkTo)
            // .WithRetry(TimeSpan.FromSeconds(30), maxAttempts: 5, parkTo: "queue.failed")
            .AutoDelete()
            .Durable()
            // Type checks
            .BoundTo(exchange);

        // Type checks
        connection.AddConsumer(queue)
            // .Tagged("tag")
            // .Prefetch(1)  // defaults to 10; 0 means unlimited
            // .Count(1)
            // .Exclusive()
            // .NoLocal()
            // .AutoAck()
            // .Requeue()
            // .Priority(2)
            .Count(2)
            .Requeue()
            .Subscribe<Payload, Consumer>();

        // Type checks
        connection.AddProducer<Payload>(exchange)
            // .Transactional()
            // .Mandatory()
            // .MessageTimeToLive(TimeSpan.FromSeconds(10))
            // .MessageId("...")
            // .CorrelationId("...")
            .Persistent();
    }));
    
public class Consumer : IConsumer<Payload> 
{
    public ValueTask HandleAsync(Payload message, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Received: {message}");
        return ValueTask.CompletedTask;
    }
}

// Get or inject
var producer = serviceProvider.GetRequiredService<IRabbitProducer>();

// Use; completes after the broker confirms the message
await producer.PublishAsync(new Payload(), overrides => ...);

// Per-message identifiers via overrides
await producer.PublishAsync(payload, b => b
    .MessageId(eventId)
    .CorrelationId(taskId));
```

#### Sample configuration appsettings.json for using Vault

```json lines
{
  "RabbitMQ": {
    "HostName": "localhost",
    "Port": 5672,
    "VirtualHost": "/",
    "UserName": "guest",
    "Password": "guest",
    "ClientProvidedName": "default",
    "Ssl": {
      "Enabled": false,
      "ServerName": null,
      "CertPath": null,
      "CertPassphrase": null
    }
  }
}
```

Every key is optional. `Port` left out means the client default — 5672 plain, 5671 once
`Ssl:Enabled` is set — and `Ssl:ServerName` left out falls back to `HostName`, which is what
certificate validation compares against. `CertPath` is only needed for mutual TLS.

## Supported

- Durable, internal, dead letter, bound and alternate exchanges
- Lazy, durable, quorum and exclusive queues
- Publisher confirmations (on by default for non-transactional producers) and transactional channel mode
- Poison message handling: undeserializable payloads are parked, dropped or rejected — never retried, never stalling
- Retry with backoff through a companion retry queue (`WithRetry`), with optional parking (`parkTo`)
- Automatic eviction and replacement of broker-closed producer channels
- Consumer configuration validation at startup and graceful consumer shutdown
- Message id and correlation id declaration
- Persistent producers
- Consumers priority
- Queue and message time to live
- Max message count and size limitations
- Batch produce
- App id declaration
- Reject-publish when queue is full
- Deleted operations
- NoWait operations

## Queues

- Declare durable queue with `Durable` extension
- Declare exclusive queue with `Exclusive` extension
- Declare queue without waiting with `NoWait` extension
- Bind exchange to exchange with `BoundTo` extension
- Declare lazy queue with `Lazy` extension
- Set queue max message count with `MaxMessageCount` extension
- Set queue max message size in bytes with `MaxMessageSize` extension
- Set dead letter exchange with `DeadLetterTo` extension
- Enable retry with backoff with `WithRetry` extension: failed messages wait in a companion
  `{queue}.retry` queue for the given delay and are redelivered; after `maxAttempts` the message
  moves to the `parkTo` queue for manual inspection, or is acknowledged, logged and dropped when
  no parking queue is configured. Cannot be combined with `DeadLetterTo`, `MessageTimeToLive`,
  `MaxMessageCount` or `MaxMessageSize`: the broker takes the queue's dead letter road for every reason, and an
  expired or overflowed message would cycle through the retry queue forever. Companion queues mirror
  the main queue's durability
- Set queue ttl with `TimeToLive` extension
- Set message ttl with `MessageTimeToLive` extension (both at most `int.MaxValue` milliseconds, 24.85 days,
  which is what the broker reads)
- Set queue max priority with `MaxPriority` extension
- Explicitly delete queue with `Deleted` extension
- Delete queue automatically with `AutoDelete` extension
- Add custom arguments with `Argument` extension

## Exchanges

- Declare durable exchange with `Durable` extension
- Declare exchange without waiting with `NoWait` extension
- Delete exchange automatically with `AutoDelete` extension
- Explicitly delete exchange with `Deleted` extension
- Bind exchange to exchange with `BoundTo` extension
- Declare alternate exchange with `AlternateTo` extension
- Add custom arguments with `Argument` extension
- Declare exchange with `AddDirectExchange(...)`, `AddFanoutExchange(...)`, `AddHeadersExchange(...)`, `AddTopicExchange(...)` extensions

## Consumers

- Tag consumers using `Tagged` extension
- Limit prefetch count with `Prefetch` extension (defaults to 10; pass 0 for an unlimited prefetch)
- Scale consumers by using `Count` extension: the consumers of one declaration share a channel whose dispatcher
  gets one worker per consumer, so `Count(4)` handles four deliveries at once
- Declare exclusive consume with `Exclusive` extension
- Forbid to consume own producers with `NoLocal` extension
- When no need to ack explicitly use `AutoAck` extension
- Requeue messages on fail with `Requeue` extension (single immediate requeue; prefer `WithRetry` on the queue).
  Messages are always settled one by one — batch ack/nack is not offered, because the consumers of one
  declaration share a channel and would settle each other's in-flight deliveries
- Set consumer priority with `Priority` extension
- Add custom arguments with `Argument` extension
- All consumers start in `IHostedService` and are cancelled cleanly when the host stops
- A message whose payload cannot be deserialized never stalls the consumer and never enters
  the retry cycle (the failure is deterministic): it is parked when the queue has a `parkTo`
  queue, acknowledged and dropped when the queue retries without one, and rejected without
  requeue otherwise (dead-lettered when the queue has a dead letter exchange)
- `AutoAck` combined with `Requeue` or a `WithRetry` queue fails the application start:
  an auto-acknowledged message can never be requeued or retried
- A handler cancelled by the host stopping has its message returned to the queue (nack with requeue) for the
  next consumer: a shutdown is neither an attempt of the retry budget nor a rejection

## Producers

- Set routing key `RoutingKey` extension
- Set mandatory with `Mandatory` extension; combined with publisher confirmations an unroutable
  message makes `PublishAsync` throw `PublishException`
- Set message priority with `Priority` extension
- Set message ttl with `MessageTimeToLive` extension
- Set message id with `MessageId` extension (per message via `PublishAsync` overrides)
- Set correlation id with `CorrelationId` extension (per message via `PublishAsync` overrides)
- Use channel transactional mode with `Transactional` extension; transactional publishes are
  serialized per channel, because an AMQP transaction is channel-wide state
- Use message persistence with `Persistent` extension
- Configure headers with `Header` extension — a publish carries no argument table of its own,
  so per-message key/value data goes here
- Configure properties with `Property` extension
- Non-transactional producers publish with confirmations: `PublishAsync` completes after the broker
  confirms the message and throws when it is rejected — it returns a plain `ValueTask`, success is
  the absence of an exception
- A publish that finds its connection lost — the socket gone, or the broker closing the connection
  (a restart, an operator) — waits for the client's automatic recovery and publishes once more.
  `options.UseRecoveryWait(TimeSpan.FromSeconds(10))` shortens the wait, `TimeSpan.Zero` fails at once.
  A channel the broker closed for a protocol error (an unknown exchange, a failed precondition) is not
  waited on: the same publish would fail again. Delivery stays at least once — a message the broker
  received just before the drop may arrive twice, so set a `MessageId` where duplicates matter

## Tracing

`RabbitMQ.Client` already emits a producer span per publish and a consumer span per delivery, and
carries the trace context between them in the message headers — a handler runs inside the span of
the publish that caused it. This library adds no span of its own; it records on the delivery span
the one thing the client cannot know, how the message was settled:

```csharp
services.AddOpenTelemetry().WithTracing(tracing => tracing.AddSource(RabbitTracing.SourceNames));
```

- `snail.messaging.settlement` — `acked`, `requeued`, `rejected`, `retry_scheduled`, `parked`,
  `dropped`, or `lost` when an auto-acknowledged message failed after the broker had settled it
- `snail.messaging.attempt` — the 1-based attempt a retried delivery is on
- `snail.messaging.poison` — set when the payload could not be deserialized

A failed delivery also carries the exception and an error status, so a retry cycle is visible as a
chain of traces rather than as silence. With no collector listening the client starts no activity
and the whole thing costs one null check per delivery.

## Limitations

- No dynamic topology declaration by design, but you can use `IRabbitConnectionProvider` for that

## Tips

- `RoutingKey` is `QueueName`/`ExchangeName` by default
- Do not use same connection for consumers and producers because of tcp backpressure
- RabbitMQ 4 rejects transient non-exclusive queues by default — declare consumer queues `Durable`
- Pass `parkTo` to `WithRetry` to keep exhausted and poison messages for manual inspection;
  without it they are logged and dropped

## Upgrading from 1.x

2.0 tightens the public API; what a 1.x caller has to change:

- `IRabbitConnectionProvider.FromDeclaration` and `IRabbitChannelProvider.FromDeclaration` are
  `FromDeclarationAsync`
- `RabbitConsumerDeclaration.QueueDeclarations` and `.Subscriptions`, `RabbitProducerDeclaration.Properties`,
  and the `BindingDeclarations` of queues and exchanges are `IReadOnlyList<T>`: add subscriptions,
  property actions and bindings through the builders (`Subscribe`, `Property`, `Header`, `BoundTo`),
  not through the lists; `RabbitProducerDeclaration.Properties` has no setter
- `RabbitClientOptions`, `RabbitClientSslOptions`, `RabbitConnectionDeclaration` and
  `RabbitMissingDeclarationException` are sealed
- New, nothing to change: `UseConnectionUrl(Uri)` beside the string overload, and `UseRecoveryWait`

Behaviour that changed with 2.0, nothing to change in code:

- `Count(n)` handles *n* deliveries at once (the consumer channel gets one dispatcher worker per consumer)
- a handler cancelled by the host stopping has its message returned to the queue, not rejected or counted as an attempt
- `WithRetry` refuses `MessageTimeToLive`, `MaxMessageCount` and `MaxMessageSize` on the same queue, and durations
  above 24.85 days are refused instead of being cut short
- a publish on a lost connection waits for the recovery, see [Producers](#producers)

## License

Snail.Toolkit.RabbitMQ is a free and open source project, released under the permissible [MIT license](LICENSE).