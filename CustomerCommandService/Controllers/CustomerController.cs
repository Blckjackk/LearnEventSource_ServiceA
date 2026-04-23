using System.Text;
using System.Text.Json;
using CustomerCommandService.Data;
using CustomerCommandService.Models;
using EventStore.Client;
using Microsoft.AspNetCore.Mvc;

namespace CustomerCommandService.Controllers;

[Route("api/[controller]")]
[ApiController]
public class CustomerController : ControllerBase
{
    private readonly IEventStoreWriter _eventStoreWriter;
    private readonly EventStoreClient _eventStoreClient;
    
    public CustomerController(
        IEventStoreWriter eventStoreWriter,
        EventStoreClient eventStoreClient)
    {
        _eventStoreWriter = eventStoreWriter;
        _eventStoreClient = eventStoreClient;
    }

    [HttpPost]
    public async Task<IActionResult> CreateCustomer([FromBody] Customer customer, CancellationToken cancellationToken)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(customer.Name))
            {
                return BadRequest("Customer name is required.");
            }

            customer.Id = Guid.NewGuid();

            var evt = new CustomerCreatedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = customer.Id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerCreatedV1",
                Data = new CustomerEventData
                {
                    Name = customer.Name,
                    Phone = customer.Phone ?? string.Empty,
                    Address = customer.Address ?? string.Empty,
                    City = customer.City ?? string.Empty,
                    Country = customer.Country ?? string.Empty,
                    Email = customer.Email ?? string.Empty
                }
            };

            await _eventStoreWriter.AppendCustomerCreatedAsync(evt, cancellationToken);

            return Accepted(new
            {
                message = "CustomerCreated event stored",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Error: {ex.Message}");
        }
    }

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetCustomer(Guid id, CancellationToken cancellationToken)
    {
        try
        {
            var aggregate = await LoadCustomerStateAsync(id, cancellationToken);
            if (aggregate is null)
            {
                return NotFound($"Customer {id} not found.");
            }

            return Ok(aggregate.Customer);
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
            var aggregate = await LoadCustomerStateAsync(id, cancellationToken);
            if (aggregate is null)
            {
                return NotFound($"Customer {id} not found.");
            }

            var existing = aggregate.Customer;
            if (existing.IsDeleted)
            {
                return Conflict($"Customer {id} is deleted and cannot be updated.");
            }

            if (string.IsNullOrWhiteSpace(customer.Name))
            {
                return BadRequest("Customer name is required.");
            }

            var evt = new CustomerUpdatedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerUpdatedV1",
                Data = new CustomerEventData
                {
                    Name = customer.Name,
                    Phone = customer.Phone ?? string.Empty,
                    Address = customer.Address ?? string.Empty,
                    City = customer.City ?? string.Empty,
                    Country = customer.Country ?? string.Empty,
                    Email = customer.Email ?? string.Empty
                }
            };

            await _eventStoreWriter.AppendCustomerUpdatedAsync(evt, aggregate.LastRevision, cancellationToken);

            return Accepted(new
            {
                message = "CustomerUpdated event stored",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (WrongExpectedVersionException)
        {
            return Conflict("Customer has changed by another request. Please retry with latest state.");
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
            var aggregate = await LoadCustomerStateAsync(id, cancellationToken);
            if (aggregate is null)
            {
                return NotFound($"Customer {id} not found.");
            }

            var existing = aggregate.Customer;
            if (existing.IsDeleted)
            {
                return Accepted(new
                {
                    message = "Customer already deleted",
                    customerId = id
                });
            }

            var evt = new CustomerDeletedEvent
            {
                EventId = Guid.NewGuid(),
                AggregateId = id,
                OccurredAtUtc = DateTime.UtcNow,
                SchemaVersion = 1,
                EventType = "CustomerDeletedV1"
            };

            await _eventStoreWriter.AppendCustomerDeletedAsync(evt, aggregate.LastRevision, cancellationToken);

            return Accepted(new
            {
                message = "CustomerDeleted event stored",
                eventId = evt.EventId,
                customerId = evt.AggregateId
            });
        }
        catch (WrongExpectedVersionException)
        {
            return Conflict("Customer has changed by another request. Please retry with latest state.");
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Error: {ex.Message}");
        }
    }

    private async Task<CustomerAggregateState?> LoadCustomerStateAsync(Guid id, CancellationToken cancellationToken)
    {
        var streamName = $"customer-{id:N}";
        var firstPage = _eventStoreClient.ReadStreamAsync(
            Direction.Forwards,
            streamName,
            StreamPosition.Start,
            maxCount: 1,
            cancellationToken: cancellationToken);

        var state = await firstPage.ReadState;
        if (state == ReadState.StreamNotFound)
        {
            return null;
        }

        var customer = new Customer { Id = id };
        ulong? lastRevision = null;

        var position = StreamPosition.Start;
        while (true)
        {
            var page = _eventStoreClient.ReadStreamAsync(
                Direction.Forwards,
                streamName,
                position,
                maxCount: 512,
                cancellationToken: cancellationToken);

            var hasAny = false;
            await foreach (var resolvedEvent in page)
            {
                hasAny = true;
                position = resolvedEvent.Event.EventNumber + 1;
                lastRevision = resolvedEvent.Event.EventNumber.ToUInt64();

                // Skip metadata and system events
                if (resolvedEvent.Event.EventType.StartsWith("$", StringComparison.Ordinal))
                {
                    continue;
                }

                var json = Encoding.UTF8.GetString(resolvedEvent.Event.Data.ToArray());

                try
                {
                    switch (resolvedEvent.Event.EventType)
                    {
                        case "CustomerCreatedV1":
                        {
                            var evt = JsonSerializer.Deserialize<CustomerCreatedEvent>(json);
                            if (evt?.Data is null)
                            {
                                continue;
                            }

                            customer.Name = evt.Data.Name;
                            customer.Phone = evt.Data.Phone ?? string.Empty;
                            customer.Address = evt.Data.Address ?? string.Empty;
                            customer.City = evt.Data.City ?? string.Empty;
                            customer.Country = evt.Data.Country ?? string.Empty;
                            customer.Email = evt.Data.Email ?? string.Empty;
                            customer.IsDeleted = false;
                            customer.UpdatedAtUtc = evt.OccurredAtUtc;
                            break;
                        }
                        case "CustomerUpdatedV1":
                        {
                            var evt = JsonSerializer.Deserialize<CustomerUpdatedEvent>(json);
                            if (evt?.Data is null)
                            {
                                continue;
                            }

                            customer.Name = evt.Data.Name;
                            customer.Phone = evt.Data.Phone ?? string.Empty;
                            customer.Address = evt.Data.Address ?? string.Empty;
                            customer.City = evt.Data.City ?? string.Empty;
                            customer.Country = evt.Data.Country ?? string.Empty;
                            customer.Email = evt.Data.Email ?? string.Empty;
                            customer.IsDeleted = false;
                            customer.UpdatedAtUtc = evt.OccurredAtUtc;
                            break;
                        }
                        case "CustomerDeletedV1":
                        {
                            var evt = JsonSerializer.Deserialize<CustomerDeletedEvent>(json);
                            if (evt is null)
                            {
                                continue;
                            }

                            customer.IsDeleted = true;
                            customer.UpdatedAtUtc = evt.OccurredAtUtc;
                            break;
                        }
                    }
                }
                catch (JsonException ex)
                {
                    // Log and continue if deserialization fails
                    Console.WriteLine($"Failed to deserialize event {resolvedEvent.Event.EventType}: {ex.Message}");
                    continue;
                }
            }

            if (!hasAny)
            {
                break;
            }
        }

        if (lastRevision is null)
        {
            return null;
        }

        return new CustomerAggregateState(customer, lastRevision.Value);
    }

    private sealed record CustomerAggregateState(Customer Customer, ulong LastRevision);

}
