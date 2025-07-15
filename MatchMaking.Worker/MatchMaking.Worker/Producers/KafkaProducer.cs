using Confluent.Kafka;
using MatchMaking.Worker.Models;
using Microsoft.Extensions.Options;

namespace MatchMaking.Worker.Producers;

public class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<Null, string> producer;

    public KafkaProducer(IOptions<KafkaSettings> options)
    {
        var configMap = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers
        };
        producer = new ProducerBuilder<Null, string>(configMap).Build();
    }

    public async Task ProduceAsync(string topic, string message)
    {
        await producer.ProduceAsync(topic, new() { Value = message });
    }
}