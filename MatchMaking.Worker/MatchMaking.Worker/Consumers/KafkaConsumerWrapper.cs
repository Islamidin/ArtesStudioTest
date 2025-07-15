using Confluent.Kafka;
using MatchMaking.Worker.Models;
using Microsoft.Extensions.Options;

namespace MatchMaking.Worker.Consumers;

public class KafkaConsumerWrapper : IKafkaConsumerWrapper
{
    private readonly IConsumer<Ignore, string> consumer;

    public KafkaConsumerWrapper(IOptions<KafkaSettings> options)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = options.Value.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        consumer = new ConsumerBuilder<Ignore, string>(config).Build();
    }

    public void Subscribe(string topic) => consumer.Subscribe(topic);

    public ConsumeResult<Ignore, string> Consume(CancellationToken token) => consumer.Consume(token);

    public void Close() => consumer.Close();
}