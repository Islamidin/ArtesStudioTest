using System.Text.Json;
using Confluent.Kafka;
using MatchMaking.Service.Consumers;
using MatchMaking.Service.Models;
using MatchMaking.Service.Store;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;

namespace MatchMaking.Service.Tests.Consumers;

[TestFixture]
public class MatchCompleteConsumerTests
{
    private MatchCompleteConsumer consumer = null!;
    private IKafkaConsumerWrapper kafka = null!;
    private ILogger<MatchCompleteConsumer> logger = null!;
    private IMatchStore matchStore = null!;

    [SetUp]
    public void SetUp()
    {
        kafka = Substitute.For<IKafkaConsumerWrapper>();
        matchStore = Substitute.For<IMatchStore>();
        logger = Substitute.For<ILogger<MatchCompleteConsumer>>();

        consumer = new(kafka, matchStore, logger);
    }

    [Test]
    public async Task ExecuteAsync_ValidMessage_StoresMatch()
    {
        var match = MakeMatchInfo();
        var json = JsonSerializer.Serialize(match);

        kafka.Consume(Arg.Any<CancellationToken>())
             .Returns(new ConsumeResult<Ignore, string>
             {
                 Message = new() { Value = json }
             });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await consumer.StartAsync(cts.Token);

        await matchStore.Received().StoreAsync(Arg.Is<MatchInfo>(m => m.MatchId == match.MatchId), json);
    }

    [Test]
    public async Task ExecuteAsync_InvalidJson_LogsWarningAndSkips()
    {
        const string invalidJson = "{ this is not json }";

        kafka.Consume(Arg.Any<CancellationToken>())
             .Returns(new ConsumeResult<Ignore, string>
             {
                 Message = new() { Value = invalidJson }
             });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await consumer.StartAsync(cts.Token);

        await matchStore.DidNotReceive().StoreAsync(Arg.Any<MatchInfo>(), Arg.Any<string>());
        logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<JsonException>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task ExecuteAsync_EmptyMessage_Skips()
    {
        kafka.Consume(Arg.Any<CancellationToken>())
             .Returns(new ConsumeResult<Ignore, string>
             {
                 Message = new() { Value = "" }
             });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await consumer.StartAsync(cts.Token);

        await matchStore.DidNotReceive().StoreAsync(Arg.Any<MatchInfo>(), Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_ConsumeThrows_LogsError()
    {
        kafka.Consume(Arg.Any<CancellationToken>())
             .Throws(new ConsumeException(null, new(ErrorCode.BrokerNotAvailable)));

        var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await consumer.StartAsync(cts.Token);

        logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<ConsumeException>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task ExecuteAsync_Cancelled_ClosesKafka()
    {
        kafka.Consume(Arg.Any<CancellationToken>())
             .Returns(_ =>
             {
                 Thread.Sleep(10);
                 return null!;
             });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        await consumer.StartAsync(cts.Token);

        kafka.Received().Close();
    }

    private static MatchInfo MakeMatchInfo() => new("match id", ["user1", "user2"]);
}