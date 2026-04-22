using CustomerCommandService.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CustomerCommandService.Messaging;
using CustomerCommandService.Data;
using EventStore.Client;


namespace CustomerCommandService.Controllers;

[Route("api/[controller]")]
[ApiController]
public class CustomerController : ControllerBase
{
    private readonly IEventStoreWriter _eventStoreWriter;
    private readonly RabbitMqPublisher _publisher;
    private readonly AppDbContext _db;
    private readonly EventStoreClient _eventStoreClient;
    
    public CustomerController(
        IEventStoreWriter eventStoreWriter,
        RabbitMqPublisher publisher,
        AppDbContext db,
        EventStoreClient eventStoreClient)
    {
        _eventStoreWriter = eventStoreWriter;
        _publisher = publisher;
        _db = db;
        _eventStoreClient = eventStoreClient;
    }

    [HttpPost]
    public async Task<IActionResult> CreateCustomer([FromBody] Customer customer, CancellationToken cancellationToken)
    {
        try
        {
            customer.Id = Guid.NewGuid();
            customer.IsDeleted = false;
            customer.UpdatedAtUtc = DateTime.UtcNow;

            _db.Customers.Add(customer);
            await _db.SaveChangesAsync(cancellationToken);

            var evt = new CustomerCreatedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = customer.Id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerCreatedV1",
                Data = new CustomerEventData
                {
                    Name = customer.Name ?? string.Empty,
                    Phone = customer.Phone ?? string.Empty,
                    Address = customer.Address ?? string.Empty,
                    City = customer.City ?? string.Empty,
                    Country = customer.Country ?? string.Empty,
                    Email = customer.Email ?? string.Empty
                }
            };

            await _eventStoreWriter.AppendCustomerCreatedAsync(evt, cancellationToken);
            _publisher.Publish(evt);

            return Accepted(new
            {
                message = "Event stored and published",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Error: {ex.Message}");
        }
    }

    [HttpPut("{id:guid}")]
    public async Task<IActionResult> UpdateCustomer(Guid id, [FromBody] Customer customer, CancellationToken cancellationToken)
    {
        try
        {
            var existing = await _db.Customers.FindAsync([id], cancellationToken);
            if (existing is null)
            {
                var hasHistory = await HasHistoryInEventStoreAsync(id, cancellationToken);
                if (!hasHistory)
                {
                    return NotFound($"Customer {id} not found.");
                }

                existing = new Customer { Id = id };
                _db.Customers.Add(existing);
            }

            existing.Name = customer.Name ?? string.Empty;
            existing.Phone = customer.Phone ?? string.Empty;
            existing.Address = customer.Address ?? string.Empty;
            existing.City = customer.City ?? string.Empty;
            existing.Country = customer.Country ?? string.Empty;
            existing.Email = customer.Email ?? string.Empty;
            existing.IsDeleted = false;
            existing.UpdatedAtUtc = DateTime.UtcNow;

            await _db.SaveChangesAsync(cancellationToken);

            var evt = new CustomerUpdatedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerUpdatedV1",
                Data = new CustomerEventData
                {
                    Name = existing.Name,
                    Phone = existing.Phone,
                    Address = existing.Address,
                    City = existing.City,
                    Country = existing.Country,
                    Email = existing.Email
                }
            };

            await _eventStoreWriter.AppendCustomerUpdatedAsync(evt, cancellationToken);
            _publisher.Publish(evt);

            return Accepted(new
            {
                message = "Update event stored and published",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Error: {ex.Message}");
        }
    }

    [HttpDelete("{id:guid}")]
    public async Task<IActionResult> DeleteCustomer(Guid id, CancellationToken cancellationToken)
    {
        try
        {
            var existing = await _db.Customers.FindAsync([id], cancellationToken);
            if (existing is null)
            {
                var hasHistory = await HasHistoryInEventStoreAsync(id, cancellationToken);
                if (!hasHistory)
                {
                    return NotFound($"Customer {id} not found.");
                }

                existing = new Customer
                {
                    Id = id,
                    Name = string.Empty,
                    Phone = string.Empty,
                    Address = string.Empty,
                    City = string.Empty,
                    Country = string.Empty,
                    Email = string.Empty,
                    IsDeleted = false
                };
                _db.Customers.Add(existing);
            }

            if (existing.IsDeleted)
            {
                return Accepted(new
                {
                    message = "Customer already deleted",
                    customerId = id
                });
            }

            existing.IsDeleted = true;
            existing.UpdatedAtUtc = DateTime.UtcNow;
            await _db.SaveChangesAsync(cancellationToken);

            var evt = new CustomerDeletedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerDeletedV1"
            };

            await _eventStoreWriter.AppendCustomerDeletedAsync(evt, cancellationToken);
            _publisher.Publish(evt);

            return Accepted(new
            {
                message = "Delete event stored and published",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Error: {ex.Message}");
        }
    }

    [HttpGet]
    public IActionResult GetCustomers()
    {
        var customer = _db.Customers.Where(x => !x.IsDeleted).ToList();
        return Ok(customer);
    }

    private async Task<bool> HasHistoryInEventStoreAsync(Guid id, CancellationToken cancellationToken)
    {
        var streamName = $"customer-{id:N}";
        var readResult = _eventStoreClient.ReadStreamAsync(
            Direction.Forwards,
            streamName,
            StreamPosition.Start,
            maxCount: 1,
            cancellationToken: cancellationToken);

        var state = await readResult.ReadState;
        if (state == ReadState.StreamNotFound)
        {
            return false;
        }

        await foreach (var _ in readResult)
        {
            return true;
        }

        return false;
    }

}
