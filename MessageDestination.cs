namespace Mango.MessageBus;

/// <summary>A logical event destination resolved by the selected broker.</summary>
public sealed record MessageDestination
{
    public MessageDestination(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Destination name cannot be empty.", nameof(name));

        Name = name;
    }

    public string Name { get; }

    public static MessageDestination OrderCreated { get; } = new("OrderCreated");
}
