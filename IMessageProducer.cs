namespace Mango.MessageBus;

public interface IMessageProducer
{
    Task SendMessage(
        object message,
        string queueName,
        string messageId,
        RabbitMqQueueOptions? queueOptions = null);
}