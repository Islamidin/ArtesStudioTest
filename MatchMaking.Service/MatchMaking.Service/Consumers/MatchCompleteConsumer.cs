using System.Text.Json;
using MatchMaking.Service.Models;
using MatchMaking.Service.Store;

namespace MatchMaking.Service.Consumers;

public class MatchCompleteConsumer : BackgroundService
{
    private const string Topic = "matchmaking.complete";
    private readonly IKafkaConsumerWrapper kafka;
    private readonly ILogger<MatchCompleteConsumer> logger;
    private readonly IMatchStore matchStore;

    public MatchCompleteConsumer(IKafkaConsumerWrapper kafka,
                                 IMatchStore matchStore,
                                 ILogger<MatchCompleteConsumer> logger)
    {
        this.kafka = kafka;
        this.matchStore = matchStore;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        kafka.Subscribe(Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = kafka.Consume(stoppingToken);
                    if (string.IsNullOrWhiteSpace(result.Message?.Value))
                    {
                        continue;
                    }

                    var match = JsonSerializer.Deserialize<MatchInfo>(result.Message.Value);
                    if (match == null)
                    {
                        logger.LogWarning("Failed to deserialize match message: {Message}", result.Message.Value);
                        continue;
                    }

                    await matchStore.StoreAsync(match, result.Message.Value).ConfigureAwait(false);

                    logger.LogInformation(
                        "Stored match info for matchId {MatchId}, users: {UserIds}",
                        match.MatchId,
                        string.Join(", ", match.UserIds));
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Kafka consume exception");
                }
                catch (JsonException ex)
                {
                    logger.LogError(ex, "Failed to deserialize Kafka message");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unexpected error in consumer loop");
                }
            }
        }
        finally
        {
            kafka.Close();
        }
    }
}