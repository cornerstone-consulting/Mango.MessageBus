namespace Mango.MessageBus;

public sealed class RabbitMqSettings
{
    public string HostName { get; set; } = string.Empty;

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public string VirtualHost { get; set; } = "/";

    /// <summary>Maps logical destination names to one or more RabbitMQ queues.</summary>
    public IDictionary<string, RabbitMqDestinationSettings> Destinations { get; set; } =
        new Dictionary<string, RabbitMqDestinationSettings>(StringComparer.OrdinalIgnoreCase);
}

