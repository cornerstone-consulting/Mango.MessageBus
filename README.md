# Mango.MessageBus

`Mango.MessageBus` 4.0.0 introduces a broker-neutral publishing contract and Kafka publishing. This is a breaking major-version update from 3.0.0. Consumers remain the responsibility of consuming services; this package does not include a consumer implementation.

## Contract and destinations

Publish through `IMessageProducer.PublishAsync`:

```csharp
await producer.PublishAsync(
    orderCreatedEvent,
    MessageDestination.OrderCreated,
    orderCreatedEvent.EventId.ToString(),
    cancellationToken);
```

A `MessageDestination` names a logical event destination. The selected provider maps it to infrastructure:

- RabbitMQ maps one logical destination to one or more queues. `OrderCreated` can therefore reach the existing EmailAPI and RewardAPI queues from one call.
- Kafka maps it to one topic. For this exercise, configure the shared `OrderCreated` topic; RewardAPI and EmailAPI must later consume it with distinct consumer groups.

The message ID is mandatory. RabbitMQ sets the AMQP `MessageId`; Kafka uses it as the record key and a `messageId` header. Both providers serialize the value as JSON with Newtonsoft.Json.

## Provider selection and settings

Register the selected producer once:

```csharp
builder.Services.AddMessageBus(builder.Configuration);
```

Set `MessageBus:Provider` to `RabbitMQ` or `Kafka` (case-insensitive). Only the selected provider's settings are bound and validated at startup. Unsupported or missing provider values and incomplete selected-provider settings fail with a clear configuration error.

Example RabbitMQ configuration:

```json
{
  "MessageBus": {
    "Provider": "RabbitMQ",
    "RabbitMQ": {
      "HostName": "rabbitmq",
      "Port": 5672,
      "UserName": "<configured username>",
      "Password": "<configured password>",
      "VirtualHost": "/",
      "Destinations": {
        "OrderCreated": {
          "Queues": [
            {
              "Name": "emailordercreated",
              "Durable": true,
              "Arguments": {
                "x-dead-letter-exchange": "email.dlx",
                "x-dead-letter-routing-key": "emailordercreated"
              }
            },
            {
              "Name": "rewardordercreated",
              "Durable": true,
              "Arguments": {
                "x-dead-letter-exchange": "reward.dlx",
                "x-dead-letter-routing-key": "rewardordercreated"
              }
            }
          ]
        }
      }
    }
  }
}
```

Each queue mapping has its own durability and arguments, preserving different dead-letter exchanges and routing keys while retaining one logical `OrderCreated` destination.

Example Kafka configuration:

```json
{
  "MessageBus": {
    "Provider": "Kafka",
    "Kafka": {
      "BootstrapServers": "kafka:9092",
      "ClientId": "mango-order-api",
      "Destinations": {
        "OrderCreated": "OrderCreated"
      }
    }
  }
}
```

Keep credentials in environment variables or a secret store, not committed configuration. Environment variable hierarchy uses double underscores, for example `MessageBus__Kafka__BootstrapServers`.

## Delivery behavior and limits

### RabbitMQ

`RabbitMQMessageProducer` reuses one automatically recovering connection. Each configured queue is declared durable as configured, and each message is persistent. Publishing enables publisher confirmations and confirmation tracking, uses the default exchange with the queue name as routing key, and sets `mandatory: true`. Queue declaration and publish failures include the affected queue and retain the underlying exception. Cancellation is passed to connection, channel, declaration, and publish operations.

When a logical destination maps to multiple queues, the producer publishes to them sequentially. RabbitMQ has no atomic multi-queue publish here: if a later queue fails, earlier queues may already have accepted the event. The thrown error identifies the failed queue; the caller must account for possible partial fan-out and duplicate delivery when retrying.

### Kafka

`KafkaMessageProducer` reuses a Confluent.Kafka producer, enables idempotence and `acks=all`, awaits the delivery result, and passes cancellation to `ProduceAsync`. Errors include the topic and preserve the underlying exception. A timeout or cancellation can leave delivery ambiguous, and application retries can create duplicates. Producer idempotence does not provide exactly-once business processing or database effects.

### Consumers and retries

This package only publishes. Kafka consumer groups, offset commits, processing retries, dead-letter handling, and duplicate-safe side effects must be implemented by consuming services. A producer confirmation means broker acceptance according to the provider's configured acknowledgment behavior; it does not mean consumers processed the event.

## Migration from 3.0.0

Version 4.0.0 is source-breaking. Replace calls to `SendMessage(message, queueName, messageId, RabbitMqQueueOptions)` with `PublishAsync(message, MessageDestination, messageId, cancellationToken)`. Remove direct registrations of `RabbitMQMessageProducer`; use `AddMessageBus(configuration)`. Move RabbitMQ connection settings under `MessageBus:RabbitMQ`, and map logical destinations to the queues and queue declaration arguments expected by current consumers. Add `MessageBus:Provider` and Kafka settings when selecting Kafka.

OrderAPI publishes `OrderCreated` through `PublishAsync`. RewardAPI and EmailAPI consume it from RabbitMQ or from the Kafka `order-created` topic, depending on their own `MessageBus:Provider` setting.

## Requirements

- .NET 10.0.
- RabbitMQ.Client 7.2.0 for RabbitMQ publishing.
- Confluent.Kafka 2.16.0 for Kafka publishing.
- A reachable selected broker and permission to publish to its configured queues or topics.
