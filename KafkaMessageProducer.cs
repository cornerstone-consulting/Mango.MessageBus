using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System.Runtime.ExceptionServices;

namespace Mango.MessageBus;

public sealed class KafkaMessageProducer : IMessageProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly IDictionary<string, string> _destinations;
    private readonly object _lifecycleLock = new();
    private readonly TaskCompletionSource<bool> _disposeCompletion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<bool>? _publishesDrained;
    private int _activePublishes;
    private bool _shutdownStarted;

    public KafkaMessageProducer(IOptions<KafkaSettings> options)
        : this(CreateProducer(ValidateOptions(options)), options.Value.Destinations)
    {
    }

    internal KafkaMessageProducer(
        IProducer<string, string> producer,
        IDictionary<string, string> destinations)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _destinations = destinations ?? throw new ArgumentNullException(nameof(destinations));
    }

    public async Task PublishAsync(
        object message,
        MessageDestination destination,
        string messageId,
        CancellationToken cancellationToken = default)
    {
        BeginPublish();
        try
        {
            ArgumentNullException.ThrowIfNull(message);
            ArgumentNullException.ThrowIfNull(destination);
            if (string.IsNullOrWhiteSpace(messageId))
                throw new ArgumentException("Message ID cannot be empty.", nameof(messageId));

            if (!_destinations.TryGetValue(destination.Name, out var topic) ||
                string.IsNullOrWhiteSpace(topic))
            {
                throw new InvalidOperationException(
                    $"Kafka destination '{destination.Name}' has no configured topic.");
            }

            var record = new Message<string, string>
            {
                Key = messageId,
                Value = JsonConvert.SerializeObject(message),
                Headers = new Headers { { "messageId", Encoding.UTF8.GetBytes(messageId) } }
            };

            try
            {
                await _producer.ProduceAsync(topic, record, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Publishing to Kafka topic '{topic}' failed. The message may or may not have been accepted by the broker.", ex);
            }
        }
        finally
        {
            EndPublish();
        }
    }

    public void Dispose()
    {
        Task drainTask;
        var ownsShutdown = false;
        lock (_lifecycleLock)
        {
            if (_shutdownStarted)
            {
                drainTask = _disposeCompletion.Task;
            }
            else
            {
                _shutdownStarted = true;
                ownsShutdown = true;
                if (_activePublishes == 0)
                {
                    drainTask = Task.CompletedTask;
                }
                else
                {
                    _publishesDrained = new TaskCompletionSource<bool>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    drainTask = _publishesDrained.Task;
                }
            }
        }

        if (!ownsShutdown)
        {
            _disposeCompletion.Task.GetAwaiter().GetResult();
            return;
        }

        Exception? shutdownFailure = null;
        try
        {
            drainTask.GetAwaiter().GetResult();
            var pending = _producer.Flush(TimeSpan.FromSeconds(10));
            if (pending > 0)
            {
                shutdownFailure = new TimeoutException(
                    $"Kafka producer shutdown timed out with {pending} record(s) still pending.");
            }
        }
        catch (Exception ex)
        {
            shutdownFailure = ex;
        }

        try
        {
            _producer.Dispose();
        }
        catch (Exception ex)
        {
            shutdownFailure = shutdownFailure is null
                ? ex
                : new AggregateException(shutdownFailure, ex);
        }

        if (shutdownFailure is not null)
        {
            _disposeCompletion.TrySetException(shutdownFailure);
            ExceptionDispatchInfo.Capture(shutdownFailure).Throw();
        }

        _disposeCompletion.TrySetResult(true);
    }

    private void BeginPublish()
    {
        lock (_lifecycleLock)
        {
            ObjectDisposedException.ThrowIf(_shutdownStarted, this);
            _activePublishes++;
        }
    }

    private void EndPublish()
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_lifecycleLock)
        {
            _activePublishes--;
            if (_shutdownStarted && _activePublishes == 0)
                drained = _publishesDrained;
        }

        drained?.TrySetResult(true);
    }

    private static KafkaSettings ValidateOptions(IOptions<KafkaSettings> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var settings = options.Value
            ?? throw new InvalidOperationException("Kafka settings are not configured.");
        if (string.IsNullOrWhiteSpace(settings.BootstrapServers))
            throw new InvalidOperationException("Kafka BootstrapServers is not configured.");
        if (string.IsNullOrWhiteSpace(settings.ClientId))
            throw new InvalidOperationException("Kafka ClientId is not configured.");
        if (settings.Destinations is null || settings.Destinations.Count == 0 ||
            settings.Destinations.Any(pair => string.IsNullOrWhiteSpace(pair.Key) || string.IsNullOrWhiteSpace(pair.Value)))
        {
            throw new InvalidOperationException(
                "At least one valid Kafka destination-to-topic mapping is required.");
        }

        return settings;
    }

    private static IProducer<string, string> CreateProducer(KafkaSettings settings)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = settings.ClientId,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        return new ProducerBuilder<string, string>(config).Build();
    }
}
