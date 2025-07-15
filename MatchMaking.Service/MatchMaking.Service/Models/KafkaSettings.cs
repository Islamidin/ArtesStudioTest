namespace MatchMaking.Service.Models;

public record KafkaSettings
{
    public string BootstrapServers { get; init; } = null!;
    public string GroupId { get; init; } = null!;
}