using System.Text.Json;
using Confluent.Kafka;
using MatchMaking.Service.Controllers;
using MatchMaking.Service.Models;
using MatchMaking.Service.Producers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
using StackExchange.Redis;

namespace MatchMaking.Service.Tests.Controllers;

[TestFixture]
public class MatchControllerTests
{
    private MatchController controller = null!;
    private IDatabase db = null!;
    private IKafkaProducer kafkaProducer = null!;
    private ILogger<MatchController> logger = null!;
    private IConnectionMultiplexer redis = null!;

    [SetUp]
    public void SetUp()
    {
        logger = Substitute.For<ILogger<MatchController>>();
        redis = Substitute.For<IConnectionMultiplexer>();
        db = Substitute.For<IDatabase>();
        kafkaProducer = Substitute.For<IKafkaProducer>();

        redis.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);

        controller = new(logger, redis, kafkaProducer);
    }

    [Test]
    public async Task Search_ReturnsBadRequest_WhenUserIdIsMissing()
    {
        var result = await controller.Search(null!);
        Assert.That(result, Is.InstanceOf<BadRequestResult>());
    }

    [Test]
    public async Task Search_ReturnsNoContent_WhenRequestIsValid()
    {
        kafkaProducer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<string>())
            .Returns(new DeliveryResult<Null, string>());

        var result = await controller.Search("user1");
        Assert.That(result, Is.InstanceOf<NoContentResult>());
    }

    [Test]
    public async Task Search_ReturnsServerError_WhenKafkaFails()
    {
        kafkaProducer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<string>())
            .Throws(new Exception("Kafka error"));

        var result = await controller.Search("user1");
        var objectResult = result as ObjectResult;
        Assert.That(objectResult, Is.Not.Null);
        Assert.That(objectResult!.StatusCode, Is.EqualTo(500));
    }

    [Test]
    public async Task GetMatchInfo_ReturnsBadRequest_WhenUserIdIsMissing()
    {
        var result = await controller.GetMatchInfo(null!);
        Assert.That(result, Is.InstanceOf<BadRequestResult>());
    }

    [Test]
    public async Task GetMatchInfo_ReturnsNotFound_WhenNoMatch()
    {
        db.StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
          .Returns(RedisValue.Null);

        var result = await controller.GetMatchInfo("user1");
        Assert.That(result, Is.InstanceOf<NotFoundResult>());
    }

    [Test]
    public async Task GetMatchInfo_ReturnsOk_WhenMatchExists()
    {
        var match = new MatchInfo("matchId", ["user1", "user2", "user3"]);
        var matchJson = JsonSerializer.Serialize(match);

        db.StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
          .Returns(matchJson);

        var result = await controller.GetMatchInfo("user1");
        var okResult = result as OkObjectResult;
        Assert.That(okResult, Is.Not.Null);
        Assert.That(okResult!.Value, Is.InstanceOf<MatchInfo>());
        Assert.That("matchId", Is.EqualTo(((MatchInfo) okResult.Value!).MatchId));
    }
}