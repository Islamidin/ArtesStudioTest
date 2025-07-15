using MatchMaking.Worker;
using MatchMaking.Worker.Consumers;
using MatchMaking.Worker.Models;
using MatchMaking.Worker.Producers;
using MatchMaking.Worker.Store;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
                                                          ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379"));

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IMatchStore, MatchStore>();
builder.Services.AddSingleton<IKafkaConsumerWrapper, KafkaConsumerWrapper>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddHostedService<MatchmakingWorker>();

var host = builder.Build();
host.Run();