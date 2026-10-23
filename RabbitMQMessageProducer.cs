
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace Mango.MessageBus;

public sealed class RabbitMQMessageProducer : IMessageProducer
{
    private readonly RabbitMqSettings _settings;

    public RabbitMQMessageProducer(IOptions<RabbitMqSettings> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _settings = options.Value;

        if (string.IsNullOrWhiteSpace(_settings.HostName))
            throw new InvalidOperationException(
                "RabbitMQ HostName is not configured.");

        if (string.IsNullOrWhiteSpace(_settings.UserName))
            throw new InvalidOperationException(
                "RabbitMQ UserName is not configured.");

        if (string.IsNullOrWhiteSpace(_settings.Password))
            throw new InvalidOperationException(
                "RabbitMQ Password is not configured.");

        if (_settings.Port is < 1 or > 65535)
            throw new InvalidOperationException(
                "RabbitMQ Port must be between 1 and 65535.");
    }

    public async Task SendMessage(object message, string queueName)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException(
                "Queue name cannot be empty.", nameof(queueName));

        var factory = new ConnectionFactory
        {
            HostName = _settings.HostName,
            Port = _settings.Port,
            UserName = _settings.UserName,
            Password = _settings.Password,
            VirtualHost = _settings.VirtualHost
        };

        await using var connection =
            await factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var json = JsonConvert.SerializeObject(message);
        var body = Encoding.UTF8.GetBytes(json);

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: queueName,
            mandatory: false,
            body: body);
    }
}
