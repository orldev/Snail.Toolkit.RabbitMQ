# Toolkit.RabbitMQ

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
    - Poison messages (payload that cannot be deserialized) are rejected without requeue and logged —
      a single broken message never stalls a consumer
    - Retry with backoff via a companion retry queue: `WithRetry` on a queue

## Installation

```bash
dotnet add package Snail.Toolkit.RabbitMQ
```

## Usage ([examples](https://github.com/phema-team/Phema.RabbitMQ/tree/master/examples))

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
            // Failed messages are parked in "queue.retry" for 30s, then redelivered; dropped after 5 attempts
            // .WithRetry(TimeSpan.FromSeconds(30), maxAttempts: 5)
            .AutoDelete()
            .Durable()
            // Type checks
            .BoundTo(exchange);

        // Type checks
        connection.AddConsumer(queue)
            // .Tagged("tag")
            // .Prefetch(1)
            // .Count(1)
            // .Exclusive()
            // .NoLocal()
            // .AutoAck()
            // .Requeue()
            // .Priority(2)
            .Count(2)
            .Requeue()
            .Subscribe<Payload, Consumer>();
            .Subscribe(...);

        // Type cheks
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
    "UserName": "guest",
    "Password": "guest"
    "ClientProvidedName": "default"
  }
}
```

## Supported

- Durable, internal, dead letter, bound and alternate exchanges
- Lazy, durable, quorum and exclusive queues
- Publisher confirmations (on by default for non-transactional producers) and transactional channel mode
- Poison message handling: undeserializable payloads are rejected without requeue and logged
- Retry with backoff through a companion retry queue (`WithRetry`)
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
- Enable retry with backoff with `WithRetry` extension: failed messages are parked in a companion
  `{queue}.retry` queue for the given delay and redelivered; after `maxAttempts` the message is
  acknowledged, logged and dropped. Cannot be combined with `DeadLetterTo`
- Set queue ttl with `TimeToLive` extension
- Set message ttl with `MessageTimeToLive` extension
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
- Limit prefetch count with `Prefetch` extension
- Scale consumers by using `Count` extension
- Declare exclusive consume with `Exclusive` extension
- Forbid to consume own producers with `NoLocal` extension
- When no need to ack explicitly use `AutoAck` extension
- Requeue messages on fail with `Requeue` extension (single immediate requeue; prefer `WithRetry` on the queue)
- Set consumer priority with `Priority` extension
- Add custom arguments with `Argument` extension
- All consumers start in `IHostedService`
- A message whose payload cannot be deserialized is rejected without requeue and logged
  (dead-lettered when the queue has a dead letter exchange); the consumer keeps running

## Producers

- Set routing key `RoutingKey` extension
- Set mandatory with `Mandatory` extension; combined with publisher confirmations an unroutable
  message makes `PublishAsync` throw `PublishException`
- Set message priority with `Priority` extension
- Set message ttl with `MessageTimeToLive` extension
- Set message id with `MessageId` extension (per message via `PublishAsync` overrides)
- Set correlation id with `CorrelationId` extension (per message via `PublishAsync` overrides)
- Use channel transactional mode with `Transactional` extension
- Use message persistence with `Persistent` extension
- Configure headers with `Header` extension
- Configure properties with `Property` extension
- Non-transactional producers publish with confirmations: `PublishAsync` completes after the broker
  confirms the message and throws when it is rejected

## Limitations

- No dynamic topology declaration by design, but you can use `IRabbitConnectionProvider` for that

## Tips

- `RoutingKey` is `QueueName`/`ExchangeName` by default
- Do not use same connection for consumers and producers because of tcp backpressure
- RabbitMQ 4 rejects transient non-exclusive queues by default — declare consumer queues `Durable`
- Messages dropped after `WithRetry` attempts are exhausted are logged but not parked;
  add your own parking via `DeadLetterTo` on a differently shaped topology if you need to keep them

## License

Snail.Toolkit.RabbitMQ is a free and open source project, released under the permissible [MIT license](LICENSE).