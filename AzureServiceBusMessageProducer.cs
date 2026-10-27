using System;
using System.Collections.Generic;
using System.Text;

namespace Mango.MessageBus
{
    public class AzureServiceBusMessageProducer : IMessageProducer
    {
        // Note: Its equivalent to Publish message in Azure service bus.
        public Task SendMessage(
            object message,
            string queueName,
            string messageId,
            RabbitMqQueueOptions? queueOptions = null)
        {
            throw new NotSupportedException(
                "Azure Service Bus publishing is not implemented.");
        }
    }
}
