using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Mango.MessageBus;

public static class MessageBusServiceCollectionExtensions
{
    public static IServiceCollection AddMessageBus(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var provider = configuration["MessageBus:Provider"]; //switch that supports the exercise 2 goal, choosing messaging provider through configuration
        services.AddOptions<MessageBusOptions>()
            .Bind(configuration.GetSection("MessageBus"))
            .Validate(options => IsSupportedProvider(options.Provider),
                "MessageBus:Provider must be 'RabbitMQ' or 'Kafka'.")
            .ValidateOnStart();

        if (string.Equals(provider, "RabbitMQ", StringComparison.OrdinalIgnoreCase))
        {
            services.AddOptions<RabbitMqSettings>()
                .Bind(configuration.GetSection("MessageBus:RabbitMQ"))
                .Validate(HasValidRabbitSettings,
                    "MessageBus:RabbitMQ requires HostName, valid Port, UserName, Password, VirtualHost, and at least one valid destination queue mapping.")
                .ValidateOnStart();
            services.AddSingleton<IMessageProducer, RabbitMQMessageProducer>();
        }
        else if (string.Equals(provider, "Kafka", StringComparison.OrdinalIgnoreCase))
        {
            services.AddOptions<KafkaSettings>()
                .Bind(configuration.GetSection("MessageBus:Kafka"))
                .Validate(HasValidKafkaSettings,
                    "MessageBus:Kafka requires BootstrapServers, ClientId, and at least one valid destination-to-topic mapping.")
                .ValidateOnStart();
            services.AddSingleton<IMessageProducer, KafkaMessageProducer>();
        }
        else
        {
            throw new InvalidOperationException(
                $"Unsupported MessageBus:Provider '{provider ?? "<missing>"}'. Use 'RabbitMQ' or 'Kafka'.");
        }

        return services;
    }

    private static bool IsSupportedProvider(string? provider) =>
        string.Equals(provider, "RabbitMQ", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(provider, "Kafka", StringComparison.OrdinalIgnoreCase);

    private static bool HasValidRabbitSettings(RabbitMqSettings? settings) =>
        settings is not null &&
        !string.IsNullOrWhiteSpace(settings.HostName) &&
        settings.Port is >= 1 and <= 65535 &&
        !string.IsNullOrWhiteSpace(settings.UserName) &&
        !string.IsNullOrWhiteSpace(settings.Password) &&
        !string.IsNullOrWhiteSpace(settings.VirtualHost) &&
        settings.Destinations is { Count: > 0 } &&
        settings.Destinations.All(pair =>
            !string.IsNullOrWhiteSpace(pair.Key) &&
            pair.Value?.Queues is { Count: > 0 } queues &&
            queues.All(queue => !string.IsNullOrWhiteSpace(queue?.Name)));

    private static bool HasValidKafkaSettings(KafkaSettings? settings) =>
        settings is not null &&
        !string.IsNullOrWhiteSpace(settings.BootstrapServers) &&
        !string.IsNullOrWhiteSpace(settings.ClientId) &&
        settings.Destinations is { Count: > 0 } &&
        settings.Destinations.All(pair =>
            !string.IsNullOrWhiteSpace(pair.Key) &&
            !string.IsNullOrWhiteSpace(pair.Value));
}
