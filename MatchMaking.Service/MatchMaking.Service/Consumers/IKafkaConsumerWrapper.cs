using Confluent.Kafka;

namespace MatchMaking.Service.Consumers;

public interface IKafkaConsumerWrapper
{
    ConsumeResult<Ignore, string> Consume(CancellationToken cancellationToken);

    void Subscribe(string topic);

    void Close();
}