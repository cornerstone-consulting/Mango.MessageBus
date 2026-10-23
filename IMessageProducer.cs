using System;
using System.Collections.Generic;
using System.Text;

namespace Mango.MessageBus
{
    public interface IMessageProducer
    {
        // TODO: This method was used by tutor for Servcie Bus. We want unique method names & ways to gain swappability
        // Task PublishMessage(object message, string topic_queue_name);

        Task SendMessage(object message, string queueName);
    }
}
