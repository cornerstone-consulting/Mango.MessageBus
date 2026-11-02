
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace Mango.MessageBus;

public sealed class RabbitMQMessageProducer : IMessageProducer, IAsyncDisposable
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly IDictionary<string, RabbitMqDestinationSettings> _destinations;
    private readonly Func<CancellationToken, Task<IConnection>> _createConnectionAsync;
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
        _createConnectionAsync = _connectionFactory.CreateConnectionAsync;
        _destinations = settings.Destinations;
    }

    internal RabbitMQMessageProducer(
        IOptions<RabbitMqSettings> options,
        Func<CancellationToken, Task<IConnection>> createConnectionAsync)
        : this(options)
    {
        _createConnectionAsync = createConnectionAsync
            ?? throw new ArgumentNullException(nameof(createConnectionAsync));
    }

    public async Task PublishAsync(
        object message,
        MessageDestination destination,
        string messageId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(destination);

        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "Message ID cannot be empty.",
                nameof(messageId));
        }

        if (!_destinations.TryGetValue(destination.Name, out var route) ||
            route.Queues.Count == 0 || route.Queues.Any(queue => string.IsNullOrWhiteSpace(queue.Name)))
        {
            throw new InvalidOperationException(
                $"RabbitMQ destination '{destination.Name}' has no configured queues.");
        }

        var json = JsonConvert.SerializeObject(message);
        var body = Encoding.UTF8.GetBytes(json);

        await _lifecycleLock.WaitAsync(cancellationToken);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var connection = await GetConnectionAsync(cancellationToken);
            var failures = new List<Exception>();
            foreach (var queue in route.Queues)
            {
                var queueName = queue.Name;
                ThrowIfCancellationRequested(cancellationToken, destination, queueName, failures);
                try
                {
                    var channelOptions = new CreateChannelOptions(
                        publisherConfirmationsEnabled: true,
                        publisherConfirmationTrackingEnabled: true);

                    await using var channel = await connection.CreateChannelAsync(
                        channelOptions, cancellationToken);

                    try
                    {
                        await channel.QueueDeclareAsync(
                            queue: queueName,
                            durable: queue.Durable,
                            exclusive: false,
                            autoDelete: false,
                            arguments: queue.Arguments,
                            cancellationToken: cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(
                            $"RabbitMQ could not declare queue '{queueName}'. The existing queue may have incompatible durability or arguments. " +
                            "Check the publisher and consumer declarations and migrate or safely recreate the queue before upgrading.", ex);
                    }

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
                            body: body,
                            cancellationToken: cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(
                            $"Publishing to RabbitMQ queue '{queueName}' failed. The message may or may not have been accepted by the broker.", ex);
                    }
                }
                catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
                {
                    ThrowCancellation(destination, queueName, failures, ex, cancellationToken);
                }
                catch (Exception ex)
                {
                    failures.Add(new InvalidOperationException(
                        $"RabbitMQ operation for destination '{destination.Name}' and queue '{queueName}' failed. " +
                        "The message may or may not have been accepted by the broker.", ex));
                }
            }

            if (failures.Count > 0)
            {
                throw new AggregateException(
                    $"Publishing RabbitMQ destination '{destination.Name}' failed for {failures.Count} queue(s).",
                    failures);
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    private static void ThrowIfCancellationRequested(
        CancellationToken cancellationToken,
        MessageDestination destination,
        string queueName,
        IReadOnlyCollection<Exception> failures)
    {
        if (!cancellationToken.IsCancellationRequested)
            return;

        if (failures.Count == 0)
            cancellationToken.ThrowIfCancellationRequested();

        throw new OperationCanceledException(
            $"Publishing RabbitMQ destination '{destination.Name}' was canceled before queue '{queueName}' was attempted. Earlier queue failures are attached.",
            new AggregateException(failures),
            cancellationToken);
    }

    private static void ThrowCancellation(
        MessageDestination destination,
        string queueName,
        List<Exception> failures,
        OperationCanceledException cancellation,
        CancellationToken cancellationToken)
    {
        if (failures.Count == 0)
            throw cancellation;

        failures.Add(new InvalidOperationException(
            $"RabbitMQ publishing for destination '{destination.Name}' and queue '{queueName}' was canceled.",
            cancellation));
        throw new OperationCanceledException(
            $"Publishing RabbitMQ destination '{destination.Name}' was canceled. Queue failures are attached.",
            new AggregateException(failures),
            cancellationToken);
    }

    // FOLLOW-UP : Verify this connection replacement logic
    // works correctly with RabbitMQ.Client automatic recovery. Avoid replacing
    // a connection that is temporarily unavailable while recovery is in progress.
    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        if (_connection is not null)
        {
            var oldConnection = _connection;
            _connection = null;
            await oldConnection.DisposeAsync();
        }

        _connection = await _createConnectionAsync(cancellationToken);
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
