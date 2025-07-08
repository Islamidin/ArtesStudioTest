namespace MatchMaking.Worker.Models;

public record MatchmakingOptions
{
    public int UsersPerMatch { get; init; } = 3;
}