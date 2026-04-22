using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using CustomerCommandService.Models;
using CustomerCommandService.Data;

namespace CustomerCommandService.Messaging
{
    public class RabbitMqConsumer
    {
        private const string ExchangeName = "customer_events";
        private const string QueueName = "customer_created.service_a";
        private readonly IServiceProvider _serviceProvider;
        public RabbitMqConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public void Start()
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                var connection = factory.CreateConnection();
                var channel = connection.CreateModel();

                channel.ExchangeDeclare(ExchangeName, ExchangeType.Fanout, false, false, null);
                channel.QueueDeclare(QueueName, false, false, false, null);
                channel.QueueBind(QueueName, ExchangeName, routingKey: "");
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                Console.WriteLine($"✓ Queue declared and bound: {QueueName}");

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                        using var jsonDoc = JsonDocument.Parse(json);
                        if (!jsonDoc.RootElement.TryGetProperty("EventType", out var eventTypeElement))
                        {
                            Console.WriteLine("❌ EventType missing. Dropping message.");
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                            return;
                        }

                        var eventType = eventTypeElement.GetString() ?? string.Empty;

                        using var scope = _serviceProvider.CreateScope();
                        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                        if (eventType == "CustomerCreatedV1")
                        {
                            var evt = JsonSerializer.Deserialize<CustomerCreatedEvent>(json);
                            if (evt is null)
                            {
                                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                                return;
                            }

                            var exists = db.Customers.Any(c => c.Id == evt.AggregateId);
                            if (exists)
                            {
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                return;
                            }

                            db.Customers.Add(new Customer
                            {
                                Id = evt.AggregateId,
                                Name = evt.Data.Name,
                                Phone = evt.Data.Phone,
                                Address = evt.Data.Address,
                                City = evt.Data.City,
                                Country = evt.Data.Country,
                                Email = evt.Data.Email,
                                IsDeleted = false,
                                UpdatedAtUtc = evt.OccurredAtUtc
                            });
                        }
                        else if (eventType == "CustomerUpdatedV1")
                        {
                            var evt = JsonSerializer.Deserialize<CustomerUpdatedEvent>(json);
                            if (evt is null)
                            {
                                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                                return;
                            }

                            var existing = db.Customers.FirstOrDefault(c => c.Id == evt.AggregateId);
                            if (existing is null)
                            {
                                db.Customers.Add(new Customer
                                {
                                    Id = evt.AggregateId,
                                    Name = evt.Data.Name,
                                    Phone = evt.Data.Phone,
                                    Address = evt.Data.Address,
                                    City = evt.Data.City,
                                    Country = evt.Data.Country,
                                    Email = evt.Data.Email,
                                    IsDeleted = false,
                                    UpdatedAtUtc = evt.OccurredAtUtc
                                });
                            }
                            else
                            {
                                existing.Name = evt.Data.Name;
                                existing.Phone = evt.Data.Phone;
                                existing.Address = evt.Data.Address;
                                existing.City = evt.Data.City;
                                existing.Country = evt.Data.Country;
                                existing.Email = evt.Data.Email;
                                existing.IsDeleted = false;
                                existing.UpdatedAtUtc = evt.OccurredAtUtc;
                            }
                        }
                        else if (eventType == "CustomerDeletedV1")
                        {
                            var evt = JsonSerializer.Deserialize<CustomerDeletedEvent>(json);
                            if (evt is null)
                            {
                                channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                                return;
                            }

                            var existing = db.Customers.FirstOrDefault(c => c.Id == evt.AggregateId);
                            if (existing is not null)
                            {
                                existing.IsDeleted = true;
                                existing.UpdatedAtUtc = evt.OccurredAtUtc;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"⚠ Unknown event type {eventType}. Ack and skip.");
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            return;
                        }

                        db.SaveChanges();
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ ERROR in Consumer.Received: {ex.Message}");
                        Console.WriteLine($"❌ Stack Trace: {ex.StackTrace}");

                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                    }
                };

                channel.BasicConsume(QueueName, false, consumer);
                Console.WriteLine("✓ Consumer listening...");
                Console.WriteLine("=== CONSUMER READY ===\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERROR in Start(): {ex.Message}");
                Console.WriteLine($"❌ Stack Trace: {ex.StackTrace}");
            }
        }
    }
}
