using System.Text.Json;
using MatchMaking.Worker.Consumers;
using MatchMaking.Worker.Models;
using MatchMaking.Worker.Producers;
using Microsoft.Extensions.Options;

namespace MatchMaking.Worker;

public class MatchmakingWorker : BackgroundService
{
    private readonly IKafkaConsumerWrapper consumer;
    private readonly ILogger<MatchmakingWorker> logger;
    private readonly IOptions<MatchmakingOptions> options;
    private readonly IKafkaProducer producer;
    private readonly List<string> queue = [];

    public MatchmakingWorker(ILogger<MatchmakingWorker> logger,
                             IOptions<MatchmakingOptions> options,
                             IKafkaProducer producer,
                             IKafkaConsumerWrapper consumer)
    {
        this.logger = logger;
        this.options = options;
        this.producer = producer;
        this.consumer = consumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        consumer.Subscribe("matchmaking.request");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (!string.IsNullOrWhiteSpace(result.Message?.Value))
                {
                    queue.Add(result.Message.Value);
                    logger.LogInformation("User {UserId} queued", result.Message.Value);
                }

                if (queue.Count >= options.Value.UsersPerMatch)
                {
                    var match = new MatchInfo(
                        Guid.NewGuid().ToString(),
                        queue.Take(options.Value.UsersPerMatch).ToList());

                    var message = JsonSerializer.Serialize(match);
                    await producer.ProduceAsync("matchmaking.complete", message);

                    logger.LogInformation("Match created: {MatchId} with users: {Users}", match.MatchId, string.Join(",", match.UserIds));
                    queue.RemoveRange(0, options.Value.UsersPerMatch);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in matchmaking loop");
            }
        }

        consumer.Close();
    }
}