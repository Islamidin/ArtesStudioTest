using MatchMaking.Worker.Models;

namespace MatchMaking.Worker.Store;

public interface IMatchStore
{
    Task StoreAsync(MatchInfo match, string rawMessage);
}