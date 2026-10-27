
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace Mango.MessageBus;

public sealed class RabbitMQMessageProducer : IMessageProducer, IAsyncDisposable
{
    private readonly ConnectionFactory _connectionFactory;
    private IConnection? _connection;

    // FOLLOW-UP : This lock currently serializes the entire
    // publishing operation, including network I/O. Refactor connection
    // creation/replacement so the lock is held only when managing the
    // shared connection. Preserve safe disposal while sends are in flight.
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private bool _disposed;

    public RabbitMQMessageProducer(IOptions<RabbitMqSettings> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var settings = options.Value
            ?? throw new InvalidOperationException(
                "RabbitMQ settings are not configured.");

        if (string.IsNullOrWhiteSpace(settings.HostName))
            throw new InvalidOperationException("RabbitMQ HostName is not configured.");

        if (string.IsNullOrWhiteSpace(settings.UserName))
            throw new InvalidOperationException("RabbitMQ UserName is not configured.");

        if (string.IsNullOrWhiteSpace(settings.Password))
            throw new InvalidOperationException("RabbitMQ Password is not configured.");

        if (settings.Port is < 1 or > 65535)
            throw new InvalidOperationException(
                "RabbitMQ Port must be between 1 and 65535.");

        if (string.IsNullOrWhiteSpace(settings.VirtualHost))
            throw new InvalidOperationException("RabbitMQ VirtualHost is not configured.");

        _connectionFactory = new ConnectionFactory
        {
            HostName = settings.HostName,
            Port = settings.Port,
            UserName = settings.UserName,
            Password = settings.Password,
            VirtualHost = settings.VirtualHost,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };
    }

    public async Task SendMessage(
        object message,
        string queueName,
        string messageId,
        RabbitMqQueueOptions? queueOptions = null)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException(
                "Queue name cannot be empty.", nameof(queueName));

        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "Message ID cannot be empty.",
                nameof(messageId));
        }

        queueOptions ??= new RabbitMqQueueOptions();

        await _lifecycleLock.WaitAsync();

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var connection = await GetConnectionAsync();

            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);

            await using var channel =
                await connection.CreateChannelAsync(channelOptions);

            // FOLLOW-UP : Cache successfully declared queue configurations
            // per connection to avoid declaring the same queue on every send. Clear the
            // cache when the connection is replaced, and account for durability/arguments
            // so incompatible declarations are never treated as equivalent.

            try
            {
                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: queueOptions.Durable,
                    exclusive: false,
                    autoDelete: false,
                    arguments: queueOptions.Arguments);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"RabbitMQ could not declare queue '{queueName}'. " +
                    "The existing queue may have incompatible durability or arguments. " +
                    "Check the publisher and consumer declarations and migrate or " +
                    "safely recreate the queue before upgrading.",
                    ex);
            }



            var json = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(json);

            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = messageId
            };

            try
            {
                await channel.BasicPublishAsync(
                    exchange: "",
                    routingKey: queueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Publishing to RabbitMQ queue '{queueName}' failed. " +
                    "The message may or may not have been accepted by the broker.",
                    ex);
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    // FOLLOW-UP : Verify this connection replacement logic
    // works correctly with RabbitMQ.Client automatic recovery. Avoid replacing
    // a connection that is temporarily unavailable while recovery is in progress.
    private async Task<IConnection> GetConnectionAsync()
    {
        if (_connection is { IsOpen: true })
            return _connection;

        if (_connection is not null)
        {
            var oldConnection = _connection;
            _connection = null;
            await oldConnection.DisposeAsync();
        }

        _connection = await _connectionFactory.CreateConnectionAsync();
        return _connection;
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycleLock.WaitAsync();

        try
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_connection is not null)
            {
                var connection = _connection;
                _connection = null;
                await connection.DisposeAsync();
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }
}
