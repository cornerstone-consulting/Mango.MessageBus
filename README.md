# Mango.MessageBus

Shared messaging library for the Mango microservices. It provides a common `IMessageProducer` interface and a RabbitMQ-backed producer, enabling the event-driven architecture of the Mango application.

> **Version 2.0.0 is a breaking release.** The RabbitMQ producer now requires configuration through `IOptions<RabbitMqSettings>`. See [Migrating from 1.x](#migrating-from-1x).

> **Status:** `RabbitMQMessageProducer` is implemented. `AzureServiceBusMessageProducer` is currently a stub and is not usable yet. There is no config-based broker switching.

## Contents

- [GitHub Packages flow](#github-packages-flow)
- [Installation](#installation)
- [Configuration](#configuration)
- [Registration](#registration)
- [Publishing a message](#publishing-a-message)
- [Current behaviour and limitations](#current-behaviour-and-limitations)
- [Migrating from 1.x](#migrating-from-1x)
- [Troubleshooting](#troubleshooting)

## GitHub Packages flow

        Developer
            │
            │Push
            ▼
        GitHub
            │
            ▼
        GitHub Action
            │
            ▼
        dotnet pack
            │
            ▼
        dotnet nuget push
            │
            ▼
        GitHub Packages
            │
            ▼
        AuthApi
        OrderApi
        ProductApi
        RewardApi
        EmailApi

## Installation

The package is hosted on GitHub Packages. Add a NuGet source for the package owner in the consuming repository (a `NuGet.Config` next to the solution):

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="github" value="https://nuget.pkg.github.com/MdKasimS-CCG/index.json" />
  </packageSources>
</configuration>
```

Authenticate with a token that has permission to read packages. Keep tokens out of source control and out of Docker images: use user-level NuGet config, environment variables or CI secrets. Docker builds and GitHub Actions must be able to authenticate during `dotnet restore`.

Then reference the package:

```xml
<PackageReference Include="Mango.MessageBus" Version="2.0.0" />
```

## Configuration

The producer reads its settings from `RabbitMqSettings`:

| Property      | Type     | Required | Default | Meaning                              |
|---------------|----------|----------|---------|--------------------------------------|
| `HostName`    | `string` | Yes      | –       | RabbitMQ hostname or service DNS name |
| `Port`        | `int`    | No       | `5672`  | AMQP port (1–65535)                  |
| `UserName`    | `string` | Yes      | –       | RabbitMQ username                    |
| `Password`    | `string` | Yes      | –       | RabbitMQ password                    |
| `VirtualHost` | `string` | No       | `/`     | RabbitMQ virtual host                |

Settings are supplied through environment variables under the `MessageQueue:RabbitMQ` configuration section (a double underscore `__` maps to the `:` hierarchy). Values below are placeholders.

```text
MessageQueue__RabbitMQ__HostName=rabbitmq
MessageQueue__RabbitMQ__Port=5672
MessageQueue__RabbitMQ__UserName=<configured username>
MessageQueue__RabbitMQ__Password=<configured password>
MessageQueue__RabbitMQ__VirtualHost=/
```


Choose the hostname for the environment:

- **Docker Compose:** the RabbitMQ Compose service name (same network).
- **Kubernetes:** the RabbitMQ Service name reachable from the Pod.
- **Running on the host:** a host-reachable hostname or address.

## Registration

In the consuming service's `Program.cs`:

```csharp
using Mango.MessageBus;

builder.Services.Configure<RabbitMqSettings>(
    builder.Configuration.GetSection("MessageQueue:RabbitMQ"));

builder.Services.AddScoped<IMessageProducer, RabbitMQMessageProducer>();
```

Settings are validated in the producer's constructor, so a missing host, username or password, or an invalid port, throws an `InvalidOperationException` the first time the producer is resolved (not at application startup).

## Publishing a message

Inject `IMessageProducer` and call `SendMessage(object message, string queueName)`. The message is serialized to JSON (Newtonsoft.Json) and published to the named queue.

```csharp
using Mango.MessageBus;

public sealed class OrderPublisher
{
    private readonly IMessageProducer _messageProducer;

    public OrderPublisher(IMessageProducer messageProducer)
        => _messageProducer = messageProducer;

    public Task PublishAsync(OrderCreated orderCreated)
        => _messageProducer.SendMessage(orderCreated, "OrderCreatedQueue");
}
```

`OrderCreated` and `"OrderCreatedQueue"` are examples. Agree the event contract and queue name between the publishing and consuming teams, and use the same declaration properties in every service. The producer declares the queue when publishing, so its properties must match those used by any consumer declaring the same queue.

Consumers are configured separately; this package does not provide a consumer API. Publish only after the work being announced (for example, saving the order) has succeeded.

## Current behaviour and limitations

For each `SendMessage` call the RabbitMQ producer:

1. Validates the message and queue name.
2. Opens a new connection and channel.
3. Declares the queue with `durable: false`, `exclusive: false`, `autoDelete: false`.
4. Serializes the message as UTF-8 JSON.
5. Publishes to the default exchange with the queue name as routing key (`mandatory: false`).
6. Disposes the channel and connection.

Known limitations (tracked as follow-up work):

- **Connection per message:** connections are not reused, which adds overhead under load.
- **Non-durable queue:** the queue does not survive a broker restart, and a consumer declaring it as durable causes `PRECONDITION_FAILED`.
- **Non-persistent messages:** messages are not marked persistent.
- **No publisher confirms:** a successful `await` does not prove the broker accepted or routed the message.
- **No retries or dead-lettering:** retry, reconnect, dead-letter and duplicate-handling policies are not provided.
- **Startup validation:** settings are validated at first resolution rather than at startup.

Do not rely on this producer alone for guaranteed delivery.

## Migrating from 1.x

- `new RabbitMQMessageProducer()` no longer exists. Resolve `IMessageProducer` through dependency injection.
- `RabbitMQMessageProducer` is now `sealed`. Use composition instead of inheritance.
- Configuration is mandatory. Add the `MessageQueue:RabbitMQ` section and the `Configure<RabbitMqSettings>(...)` registration shown above, otherwise the producer throws when first resolved.

## Troubleshooting

**`RabbitMQ HostName is not configured`**
Check that `MessageQueue:RabbitMQ:HostName` is supplied by an active configuration provider, that `RabbitMqSettings` is bound to that section, and that the hostname is correct for the environment (Compose, Kubernetes or host).

**`RabbitMQ UserName is not configured` / `RabbitMQ Password is not configured`**
Provide non-empty credentials through environment-specific configuration or a secret manager.

**`PRECONDITION_FAILED` when declaring a queue**
Producer and consumer are declaring the same queue with different properties (for example durable vs non-durable). Align the declarations across services. Changing a declaration does not change an existing queue, so it may need to be deleted and recreated.

**Package restore cannot find `Mango.MessageBus` 2.0.0**
Verify that 2.0.0 is published to GitHub Packages, the NuGet source URL uses the correct owner, the environment has valid package-read credentials, and the `.csproj` references the published version.

**A message appears to publish but the consumer does not receive it**
Check that the queue exists, the consumer is connected and the queue is correctly bound. The producer uses `mandatory: false`, so an unroutable message is not returned to the publisher.

---
