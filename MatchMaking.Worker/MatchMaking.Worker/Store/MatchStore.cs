using MatchMaking.Worker.Models;
using StackExchange.Redis;

namespace MatchMaking.Worker.Store;

public class MatchStore : IMatchStore
{
    private readonly IConnectionMultiplexer redis;

    public MatchStore(IConnectionMultiplexer redis)
    {
        this.redis = redis;
    }

    public async Task StoreAsync(MatchInfo match, string rawMessage)
    {
        var db = redis.GetDatabase();
        var tasks = match.UserIds
                         .Select(id => db.StringSetAsync($"match:user:{id}", rawMessage))
                         .ToArray();

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
}