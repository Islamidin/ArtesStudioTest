using Confluent.Kafka;
using MatchMaking.Service.Models;
using Microsoft.Extensions.Options;

namespace MatchMaking.Service.Consumers;

public class KafkaConsumerWrapper : IKafkaConsumerWrapper, IDisposable
{
    private readonly IConsumer<Ignore, string> consumer;
    private readonly ILogger<KafkaConsumerWrapper> logger;
    private bool disposed;

    public KafkaConsumerWrapper(IOptions<KafkaSettings> options,
                                IConsumerFactory factory,
                                ILogger<KafkaConsumerWrapper> logger)
    {
        logger.LogInformation("options.Value.BootstrapServers: {ValueBootstrapServers}", options.Value.BootstrapServers);
        logger.LogInformation("options.Value.GroupId: {ValueGroupId}", options.Value.GroupId);

        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = options.Value.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,

            SocketTimeoutMs = 10000,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 300000,
            HeartbeatIntervalMs = 3000,

            ReconnectBackoffMs = 1000,
            ReconnectBackoffMaxMs = 10000
        };

        try
        {
            consumer = factory.CreateConsumer(config);
            this.logger.LogInformation("Kafka consumer initialized for group {GroupId}", options.Value.GroupId);
        }
        catch (Exception ex)
        {
            this.logger.LogCritical(ex, "Failed to create Kafka consumer");
            throw;
        }
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        try
        {
            consumer.Dispose();
            logger.LogInformation("Kafka consumer disposed");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error during consumer disposal");
        }
        finally
        {
            disposed = true;
        }
    }

    public void Subscribe(string topic)
    {
        try
        {
            consumer.Subscribe(topic);
            logger.LogInformation("Subscribed to topic {Topic}", topic);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to subscribe to topic {Topic}", topic);
            throw;
        }
    }

    public ConsumeResult<Ignore, string> Consume(CancellationToken cancellationToken)
    {
        try
        {
            var result = consumer.Consume(cancellationToken);
            logger.LogDebug("Consumed message from {Topic} [Partition: {Partition}]",
                            result.Topic, result.Partition);
            return result;
        }
        catch (ConsumeException ex)
        {
            logger.LogError(ex, "Consume error: {Reason}", ex.Error.Reason);
            throw new KafkaConsumeException("Failed to consume message", ex);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Consume operation was canceled");
            throw;
        }
    }

    public void Close()
    {
        try
        {
            consumer.Close();
            logger.LogInformation("Kafka consumer closed gracefully");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error while closing consumer");
            throw;
        }
    }

    public void Commit(ConsumeResult<Ignore, string> result)
    {
        try
        {
            consumer.StoreOffset(result);
            logger.LogDebug("Committed offset for {Topic} [Partition: {Partition}, Offset: {Offset}]",
                            result.Topic, result.Partition, result.Offset);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to commit offset");
            throw;
        }
    }
}

public class KafkaConsumeException : Exception
{
    public KafkaConsumeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}