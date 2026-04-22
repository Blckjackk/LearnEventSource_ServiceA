using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using CustomerCommandService.Models;

namespace CustomerCommandService.Messaging;

public class RabbitMqPublisher
{
    public void Publish<T>(T message)
    {
        const string exchangeName = "customer_events";

         var factory = new ConnectionFactory()
        {
            HostName = "localhost"
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: false,
            autoDelete: false,
            arguments: null
        );

        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        channel.BasicPublish(
            exchange: exchangeName,
            routingKey: "",
            basicProperties: null,
            body: body
        );
    }
}
