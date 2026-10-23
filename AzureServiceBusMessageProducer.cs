using System;
using System.Collections.Generic;
using System.Text;

namespace Mango.MessageBus
{
    public class AzureServiceBusMessageProducer : IMessageProducer
    {
        // Note: Its equivalent to Publish message in Azure service bus.
        public async Task SendMessage(object message, string queueName)
        {
            //await using var client = new ServiceBusClient();
        }
    }
}
