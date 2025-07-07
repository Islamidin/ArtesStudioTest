using MatchMaking.Service.Models;

namespace MatchMaking.Service.Store;

public interface IMatchStore
{
    Task StoreAsync(MatchInfo match, string rawMessage);
}