using System.Threading.RateLimiting;
using MatchMaking.Service.Consumers;
using MatchMaking.Service.Models;
using MatchMaking.Service.Producers;
using MatchMaking.Service.Store;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();

builder.Services.AddRateLimiter(options =>
{
    options.AddPolicy("PerUserPolicy", context =>
    {
        var userId = context.Request.Query["userId"].FirstOrDefault()
                     ?? context.User.Identity?.Name
                     ?? context.Request.Headers["X-User-Id"].FirstOrDefault()
                     ?? context.Connection.RemoteIpAddress?.ToString()
                     ?? "anonymous";

        return RateLimitPartition.GetTokenBucketLimiter(userId, _ => new()
        {
            TokenLimit = 1,
            TokensPerPeriod = 1,
            ReplenishmentPeriod = TimeSpan.FromMilliseconds(100),
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            QueueLimit = 0,
            AutoReplenishment = true
        });
    });
});

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
                                                          ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379"));

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IMatchStore, MatchStore>();
builder.Services.AddSingleton<IKafkaConsumerWrapper, KafkaConsumerWrapper>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IConsumerFactory, ConsumerFactory>();
builder.Services.AddHealthChecks();
builder.Services.AddHostedService<MatchCompleteConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.MapHealthChecks("/health");

app.Run();