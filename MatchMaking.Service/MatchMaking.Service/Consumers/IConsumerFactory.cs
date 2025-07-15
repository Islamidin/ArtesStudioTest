using Confluent.Kafka;

namespace MatchMaking.Service.Consumers;

public interface IConsumerFactory
{
    IConsumer<Ignore, string> CreateConsumer(ConsumerConfig config);
}