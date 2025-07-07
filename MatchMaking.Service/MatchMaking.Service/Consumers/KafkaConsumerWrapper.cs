using Confluent.Kafka;
using MatchMaking.Service.Models;
using Microsoft.Extensions.Options;

namespace MatchMaking.Service.Consumers;

public class KafkaConsumerWrapper : IKafkaConsumerWrapper
{
    private readonly IConsumer<Ignore, string> consumer;

    public KafkaConsumerWrapper(IOptions<KafkaSettings> options, IConsumerFactory factory)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = options.Value.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        consumer = factory.CreateConsumer(config);
    }

    public void Subscribe(string topic) => consumer.Subscribe(topic);

    public ConsumeResult<Ignore, string> Consume(CancellationToken token) => consumer.Consume(token);

    public void Close() => consumer.Close();
}