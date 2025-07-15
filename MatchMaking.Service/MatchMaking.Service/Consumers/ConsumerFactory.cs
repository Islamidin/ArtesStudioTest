using Confluent.Kafka;

namespace MatchMaking.Service.Consumers;

public class ConsumerFactory : IConsumerFactory
{
    public IConsumer<Ignore, string> CreateConsumer(ConsumerConfig config) => new ConsumerBuilder<Ignore, string>(config).Build();
}