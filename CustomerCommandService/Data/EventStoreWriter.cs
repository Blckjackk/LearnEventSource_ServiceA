using System.Text;
using System.Text.Json;
using CustomerCommandService.Models;
using EventStore.Client;

namespace CustomerCommandService.Data;

public interface IEventStoreWriter
{
    Task AppendCustomerCreatedAsync(CustomerCreatedEvent evt, CancellationToken cancellationToken = default);
    Task AppendCustomerUpdatedAsync(CustomerUpdatedEvent evt, CancellationToken cancellationToken = default);
    Task AppendCustomerDeletedAsync(CustomerDeletedEvent evt, CancellationToken cancellationToken = default);
}

public class EventStoreWriter : IEventStoreWriter
{
    private readonly EventStoreClient _client;

    public EventStoreWriter(EventStoreClient client)
    {
        _client = client;
    }

    public async Task AppendCustomerCreatedAsync(CustomerCreatedEvent evt, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        await AppendToCustomerStreamAsync(evt.AggregateId, evt.EventId, evt.EventType, payload, cancellationToken);
    }

    public async Task AppendCustomerUpdatedAsync(CustomerUpdatedEvent evt, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        await AppendToCustomerStreamAsync(evt.AggregateId, evt.EventId, evt.EventType, payload, cancellationToken);
    }

    public async Task AppendCustomerDeletedAsync(CustomerDeletedEvent evt, CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        await AppendToCustomerStreamAsync(evt.AggregateId, evt.EventId, evt.EventType, payload, cancellationToken);
    }

    private async Task AppendToCustomerStreamAsync(
        Guid aggregateId,
        Guid eventId,
        string eventType,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        var streamName = $"customer-{aggregateId:N}";

        var eventData = new EventData(
            eventId: Uuid.FromGuid(eventId),
            type: eventType,
            data: payload,
            metadata: Encoding.UTF8.GetBytes("{}")
        );

        await _client.AppendToStreamAsync(
            streamName,
            StreamState.Any,
            new[] { eventData },
            cancellationToken: cancellationToken
        );
    }
}