namespace Mango.MessageBus;

public interface IMessageProducer
{
    Task PublishAsync(
        object message,
        MessageDestination destination,
        string messageId,
        CancellationToken cancellationToken = default);
}
