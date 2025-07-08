using System.Text.Json;
using MatchMaking.Service.Models;
using MatchMaking.Service.Producers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using StackExchange.Redis;

namespace MatchMaking.Service.Controllers;

[ApiController]
[Route("match")]
public class MatchController : ControllerBase
{
    private const string MatchmakingRequestTopic = "matchmaking.request";
    private readonly IKafkaProducer kafkaProducer;
    private readonly ILogger<MatchController> logger;
    private readonly IDatabase redisDb;

    public MatchController(ILogger<MatchController> logger,
                           IConnectionMultiplexer redis,
                           IKafkaProducer kafkaProducer)
    {
        this.logger = logger;
        redisDb = redis.GetDatabase();
        this.kafkaProducer = kafkaProducer;
    }

    [EnableRateLimiting("PerUserPolicy")]
    [HttpPost("search")]
    public async Task<IActionResult> Search([FromQuery] string userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            logger.LogWarning("Match search request failed: userId is missing.");
            return BadRequest();
        }

        try
        {
            await kafkaProducer.ProduceAsync(MatchmakingRequestTopic, userId);
            logger.LogInformation("Match search request published to Kafka for userId: {UserId}", userId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish matchmaking request to Kafka for userId: {UserId}", userId);
            return StatusCode(500, "Failed to process matchmaking request.");
        }

        return NoContent();
    }

    [EnableRateLimiting("PerUserPolicy")]
    [HttpGet("info")]
    public async Task<IActionResult> GetMatchInfo([FromQuery] string userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            logger.LogWarning("Retrieve match info failed: userId is missing.");
            return BadRequest();
        }

        var matchJson = await redisDb.StringGetAsync($"match:user:{userId}");
        if (matchJson.IsNullOrEmpty)
        {
            logger.LogWarning("No match found for userId: {UserId}", userId);
            return NotFound();
        }

        var match = JsonSerializer.Deserialize<MatchInfo>(matchJson!);

        logger.LogInformation("Match info retrieved for userId: {UserId}", userId);
        return Ok(match);
    }
}