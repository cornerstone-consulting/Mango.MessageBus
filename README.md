# Mango.MessageBus

## Migrating to 3.0.0

Version 3.0.0 contains breaking changes. Review these items before upgrading any service.

- The package targets .NET 10.0. Consumers must use a compatible target framework.
- RabbitMQ configuration through `IOptions<RabbitMqSettings>` is required.
- Register `IMessageProducer` as a singleton to reuse the producer's RabbitMQ connection.
- Queue topology is supplied through `RabbitMqQueueOptions`. Remove reliance on the library's previous hardcoded RewardAPI queue configuration.
- Align queue names, durability, dead-letter exchange, dead-letter queue, routing keys, and exchange type between publishers and consumers.

### Existing RabbitMQ queues

RabbitMQ does not modify an existing queue when a declaration changes its durability or arguments. A declaration with incompatible properties can fail with `PRECONDITION_FAILED` (406), causing publishing to fail.

Before upgrading, inventory the existing queues and compare their current properties with the intended declarations. Plan a migration or safe recreation for incompatible queues. Do not blindly delete queues in shared or production environments: deleting a queue can permanently discard messages. Drain or preserve messages and coordinate the migration with all affected consumers before recreation.

### Dependency injection migration

Replace:

`AddScoped<IMessageProducer, RabbitMQMessageProducer>()`

with:

`AddSingleton<IMessageProducer, RabbitMQMessageProducer>()`

This is required for connection reuse across requests.

Test the upgraded producer and consumer together before rolling out the package to all services.

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Dependency Injection](#dependency-injection)
- [Publishing a Message](#publishing-a-message)
- [Reliability and Delivery](#reliability-and-delivery)
- [Dead-Letter Queue](#dead-letter-queue)
- [Limitations](#limitations)
- [Migrating to 3.0.0](#migrating-to-300)
- [Troubleshooting](#troubleshooting)

## Features

- Shared `IMessageProducer` abstraction.
- RabbitMQ-backed message publishing.
- JSON serialization using Newtonsoft.Json.
- Reusable RabbitMQ connection per producer instance.
- A channel created and disposed for each publishing operation.
- Durable queue declarations and persistent messages.
- Publisher confirmations and confirmation tracking.
- Mandatory publishing to detect unroutable messages.
- Caller-supplied queue declaration arguments through RabbitMqQueueOptions.
- Validation of required RabbitMQ settings.

## Requirements

- .NET 10.0.
- A reachable RabbitMQ instance.
- Access to the Mango.MessageBus package on GitHub Packages.
- A GitHub Packages token with permission to read packages.

## Installation

Configure GitHub Packages as a NuGet package source in a `NuGet.Config` file used by the consuming repository:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="github"
         value="https://nuget.pkg.github.com/cornerstone-consulting/index.json" />
  </packageSources>
</configuration>
```

Authenticate with a token that has permission to read packages. Keep credentials out of source control and Docker images. Use user-level NuGet configuration, environment variables, or CI secrets as appropriate.

Add the package reference to the consuming service's project file:

```xml
<PackageReference Include="Mango.MessageBus" Version="3.0.0" />
```

## Configuration

The producer uses `RabbitMqSettings` to configure its RabbitMQ connection.

| Property | Type | Required | Default |
|---|---|---|---|
| `HostName` | `string` | Yes | None |
| `Port` | `int` | No | `5672` |
| `UserName` | `string` | Yes | None |
| `Password` | `string` | Yes | None |
| `VirtualHost` | `string` | Yes | `/` |

Settings are read from the `MessageQueue:RabbitMQ` configuration section. When using environment variables, double underscores (`__`) represent configuration hierarchy separators.

Example environment variables:

```text
MessageQueue__RabbitMQ__HostName=rabbitmq
MessageQueue__RabbitMQ__Port=5672
MessageQueue__RabbitMQ__UserName=<configured username>
MessageQueue__RabbitMQ__Password=<configured password>
MessageQueue__RabbitMQ__VirtualHost=/
```

Replace placeholder values with your environment's configuration. Do not commit real credentials.

Choose the hostname according to the deployment environment:

- **Docker Compose:** Use the RabbitMQ service name when both services share a Compose network.
- **Kubernetes:** Use the RabbitMQ Service DNS name.
- **Local execution:** Use a hostname or address reachable from the host.

The producer validates that the hostname, username, password, and virtual host are not empty and that the port is between `1` and `65535`. Invalid settings cause an exception when the producer is constructed.

## Dependency Injection

Register the RabbitMQ settings and producer in the consuming service's `Program.cs`:

```csharp
using Mango.MessageBus;

builder.Services.Configure<RabbitMqSettings>(
    builder.Configuration.GetSection("MessageQueue:RabbitMQ"));

builder.Services.AddSingleton<IMessageProducer, RabbitMQMessageProducer>();
```
Register the producer as a singleton so its RabbitMQ connection can be reused across requests and publishing operations. Scoped registration would create separate producer instances and defeat connection reuse. The singleton is disposed by dependency injection when the application shuts down.
The producer is resolved through dependency injection. Its RabbitMQ connection is created lazily when the first message is sent, rather than during construction.

The registration above does not itself validate the configuration at application startup.

## Publishing a Message

Inject `IMessageProducer` into the class responsible for publishing events:

```csharp
using Mango.MessageBus;

public sealed class OrderPublisher
{
    private readonly IMessageProducer _messageProducer;

    public OrderPublisher(IMessageProducer messageProducer)
    {
        _messageProducer = messageProducer;
    }

    public Task PublishAsync(OrderCreated orderCreated)
    {
        return _messageProducer.SendMessage(
            orderCreated,
            "OrderCreatedQueue",
            orderCreated.EventId.ToString());
    }
}
```

`OrderCreated` and `OrderCreatedQueue` are illustrative names. Use the event contract and queue name agreed upon by the publishing and consuming services.

The producer serializes the message to JSON and publishes it to RabbitMQ's default exchange using the queue name as the routing key.

Consumers are implemented separately; this package does not provide a consumer API. Publish an event only after the underlying operation—for example, saving an order—has succeeded.

## Reliability and Delivery

For each `SendMessage` call, `RabbitMQMessageProducer`:

1. Validates the message and queue name.
2. Acquires a lifecycle semaphore to coordinate publishing, connection creation, and disposal.
3. Reuses an open RabbitMQ connection or creates a new one if no usable connection exists.
4. Creates a channel for the publishing operation and disposes it afterward.
5. Declares the target queue with durable settings.
6. Serializes the message to UTF-8 JSON.
7. Publishes a persistent message using the caller-supplied `messageId`.
8. Enables publisher confirmations and confirmation tracking, and publishes with `mandatory: true`.
9. Disposes the channel and releases the lifecycle semaphore.

### Connection lifecycle

The producer enables RabbitMQ.Client automatic connection recovery and reuses
its connection across publishing operations.

If a connection interruption occurs, RabbitMQ.Client attempts recovery.
The producer's connection lifecycle logic must be compatible with that recovery
process. Recovery behavior should be verified through integration tests,
including sends after an interruption.

### Queue durability and message persistence

Queues are declared as durable, non-exclusive, and not auto-deleted. Messages are marked persistent.

These settings improve message survival across broker restarts when RabbitMQ is configured appropriately. They do not guarantee delivery in every failure scenario or guarantee that a consumer has processed a message.

All services declaring the same queue must use compatible queue properties. An existing queue declared with different properties may cause RabbitMQ to return `PRECONDITION_FAILED`.

### Publisher confirmations and unroutable messages

Publisher confirmations are enabled with confirmation tracking, and publishing uses `mandatory: true`.

The publishing operation awaits the RabbitMQ client's publish operation. A successful result does not mean that EmailAPI, RewardAPI, or another consumer has successfully processed the event.

Publishing failures are wrapped in an `InvalidOperationException` that preserves the underlying exception.

A connection failure can occur after RabbitMQ has received a message. Blindly retrying a failed send may therefore produce duplicate deliveries. Consumers should be designed to handle duplicates where necessary.

## Dead-Letter Queue

The publishing service supplies queue declaration arguments through
`RabbitMqQueueOptions`. The shared library declares the requested main queue;
it does not hardcode service-specific dead-letter topology.

The service responsible for dead-letter topology must declare the dead-letter
exchange and queue and bind them using the agreed routing key. The publisher
and consumer must declare the main queue with compatible durability and
arguments.

For example, a service may configure `x-dead-letter-exchange` and
`x-dead-letter-routing-key` in `RabbitMqQueueOptions.Arguments`. The exact
values must match the topology agreed upon by the affected services.

## Limitations

- **No consumer implementation:** Consumers must be implemented in the consuming services.
- **No exactly-once guarantee:** Publisher confirmations do not guarantee successful processing by a consumer.
- **No general retry policy:** The package does not implement a configurable retry count, delay, or retry queue.
- **Message ID responsibility:** Callers must supply a stable message ID. The package does not generate or validate event identity beyond checking that the supplied ID is non-empty.
- **No automatic idle reconnection:** A closed connection is replaced when a later send attempts to use it.
- **Serialized publishing:** The lifecycle semaphore allows only one publishing operation at a time per producer instance.
- **No broker switching:** Azure Service Bus is not currently implemented as a usable alternative.
- **Deployment compatibility:** Existing queue declarations must be compatible with the durable and dead-letter settings used by the producer.

## Troubleshooting

### RabbitMQ HostName is not configured

Check that `MessageQueue:RabbitMQ:HostName` is supplied by an active configuration provider and that the settings are bound to the correct section.

### RabbitMQ UserName or Password is not configured

Provide non-empty credentials through environment-specific configuration or a secret manager.

### PRECONDITION_FAILED when declaring a queue

The producer and consumer may be declaring the same queue with incompatible durability or arguments. Align their declarations. If the queue already exists, plan its migration before changing its properties.

### Package restore cannot find Mango.MessageBus

Verify that the package source URL is correct, that the requested package version is available, and that the environment has valid package-read credentials.

### A message is not received by a consumer

Verify that RabbitMQ is reachable, the target queue exists, the consumer is connected, and the event contract and queue name match. Check publisher errors and RabbitMQ logs for routing or declaration failures.

### Publishing fails after a connection interruption

Inspect the underlying exception and RabbitMQ availability. The producer attempts to create a replacement connection on a later send if it detects that the existing connection has closed. Before retrying a potentially ambiguous send, consider whether the consumer can safely handle duplicate messages.
