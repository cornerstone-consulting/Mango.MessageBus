namespace Mango.MessageBus;

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = string.Empty;

    public string ClientId { get; set; } = "Mango.MessageBus";

    /// <summary>Maps logical destination names to Kafka topic names.</summary>
    public IDictionary<string, string> Destinations { get; set; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
