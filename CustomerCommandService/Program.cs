using CustomerCommandService.Data;
using CustomerCommandService.Messaging;
using EventStore.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton(sp =>
{
    var conn = builder.Configuration["EventStore:ConnectionString"]
               ?? "esdb://admin:changeit@localhost:2113?tls=false";
    return new EventStoreClient(EventStoreClientSettings.Create(conn));
});
builder.Services.AddScoped<IEventStoreWriter, EventStoreWriter>();


var app = builder.Build();


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
