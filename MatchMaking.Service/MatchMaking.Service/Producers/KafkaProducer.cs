using Confluent.Kafka;
using MatchMaking.Service.Models;
using Microsoft.Extensions.Options;

namespace MatchMaking.Service.Producers;

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

    public async Task<DeliveryResult<Null, string>> ProduceAsync(string topic, string message) => await producer.ProduceAsync(topic, new() { Value = message });
}