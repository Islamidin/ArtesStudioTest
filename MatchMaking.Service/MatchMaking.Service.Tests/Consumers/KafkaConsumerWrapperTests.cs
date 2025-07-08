using Confluent.Kafka;
using MatchMaking.Service.Consumers;
using MatchMaking.Service.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;

namespace MatchMaking.Service.Tests.Consumers;

[TestFixture]
public class KafkaConsumerWrapperTests
{
    private IConsumer<Ignore, string> consumer = null!;
    private IConsumerFactory factory = null!;
    private ILogger<KafkaConsumerWrapper> logger = null!;
    private KafkaSettings settings = null!;
    private KafkaConsumerWrapper wrapper = null!;

    [SetUp]
    public void SetUp()
    {
        settings = new() { BootstrapServers = "localhost:9092", GroupId = "test-group" };
        var options = Options.Create(settings);

        consumer = Substitute.For<IConsumer<Ignore, string>>();
        factory = Substitute.For<IConsumerFactory>();
        factory.CreateConsumer(Arg.Any<ConsumerConfig>()).Returns(consumer);
        logger = Substitute.For<ILogger<KafkaConsumerWrapper>>();

        wrapper = new(options, factory, logger);
    }

    [Test]
    public void Subscribe_CallsConsumerSubscribe()
    {
        wrapper.Subscribe("matchmaking.request");
        consumer.Received(1).Subscribe("matchmaking.request");
    }

    [Test]
    public void Subscribe_WithEmptyTopic_StillCallsConsumer()
    {
        wrapper.Subscribe(string.Empty);
        consumer.Received(1).Subscribe(string.Empty);
    }

    [Test]
    public void Consume_ReturnsMessageValue()
    {
        var token = CancellationToken.None;
        var result = new ConsumeResult<Ignore, string>
        {
            Message = new() { Value = "test-value" }
        };

        consumer.Consume(token).Returns(result);

        var actual = wrapper.Consume(token);

        Assert.That(actual.Message.Value, Is.EqualTo("test-value"));
        consumer.Received(1).Consume(token);
    }

    [Test]
    public void Close_CallsConsumerClose()
    {
        wrapper.Close();
        consumer.Received(1).Close();
    }
}