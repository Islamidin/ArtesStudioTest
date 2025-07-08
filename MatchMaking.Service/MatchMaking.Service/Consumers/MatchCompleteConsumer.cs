using System.Text.Json;
using Confluent.Kafka;
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

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            logger.LogInformation("Starting MatchCompleteConsumer");
            await base.StartAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start MatchCompleteConsumer");
            throw;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(">>> MatchCompleteConsumer ExecuteAsync started");

        try
        {
            logger.LogInformation("Attempting to subscribe to topic {Topic}", Topic);
            kafka.Subscribe(Topic);
            logger.LogInformation("Successfully subscribed to topic {Topic}", Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    logger.LogDebug("Waiting for next message...");
                    var result = kafka.Consume(stoppingToken);

                    if (string.IsNullOrWhiteSpace(result.Message?.Value))
                    {
                        logger.LogWarning("Received empty message");
                        continue;
                    }

                    logger.LogDebug("Received message: {Message}", result.Message.Value);

                    var match = JsonSerializer.Deserialize<MatchInfo>(result.Message.Value);
                    if (match == null)
                    {
                        logger.LogWarning("Failed to deserialize match message: {Message}", result.Message.Value);
                        continue;
                    }

                    logger.LogInformation("Processing match {MatchId}", match.MatchId);
                    await matchStore.StoreAsync(match, result.Message.Value).ConfigureAwait(false);

                    kafka.Commit(result);

                    logger.LogInformation(
                        "Stored match info for matchId {MatchId}, users: {UserIds}",
                        match.MatchId,
                        string.Join(", ", match.UserIds));
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Kafka consume exception. Error: {Error}", ex.Error.Reason);
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
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Fatal error in ExecuteAsync");
            throw;
        }
        finally
        {
            logger.LogInformation("Closing Kafka consumer");
            kafka.Close();
        }
    }
}