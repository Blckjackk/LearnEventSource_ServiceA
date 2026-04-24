using CustomerCommandService.Data;
using CustomerCommandService.Messaging;
using EventStore.Client;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        sqlOptions => sqlOptions.EnableRetryOnFailure()));

builder.Services.AddSingleton(sp =>
{
    var conn = builder.Configuration["EventStore:ConnectionString"]
               ?? "esdb://admin:changeit@localhost:2113?tls=false";
    return new EventStoreClient(EventStoreClientSettings.Create(conn));
});
builder.Services.AddScoped<IEventStoreWriter, EventStoreWriter>();


var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILoggerFactory>()
        .CreateLogger("Startup");

    try
    {
        db.Database.Migrate();
    }
    catch (Exception ex)
    {
        logger.LogWarning(ex, "Database migrate at startup failed. Service will keep running and command writes continue to EventStore.");
    }
}


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
