
namespace Mango.MessageBus;

public sealed class RabbitMqQueueOptions
{
	public bool Durable { get; set; } = true;

	public IDictionary<string, object?>? Arguments { get; set; }
}
