using Confluent.Kafka;

namespace MatchMaking.Service.Producers;

public interface IKafkaProducer
{
    Task<DeliveryResult<Null, string>> ProduceAsync(string topic, string message);
}