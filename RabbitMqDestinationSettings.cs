namespace Mango.MessageBus;

/// <summary>RabbitMQ-specific queue declarations for one logical destination.</summary>
public sealed class RabbitMqDestinationSettings
{
    public List<RabbitMqQueueSettings> Queues { get; set; } = new();
}

public sealed class RabbitMqQueueSettings
{
    public string Name { get; set; } = string.Empty;

    public bool Durable { get; set; } = true;

    public IDictionary<string, object?>? Arguments { get; set; }
}
