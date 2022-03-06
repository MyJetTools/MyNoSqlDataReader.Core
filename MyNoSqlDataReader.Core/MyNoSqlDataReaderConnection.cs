using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core;


public abstract class MyNoSqlDataReaderConnection
{
    private readonly Dictionary<string, IMyNoSqlDataReaderEventUpdater> _subscribers = new();

    public readonly Logger Logger = new Logger();
    
    public async Task WaitAllTablesAreInitialized()
    {
        foreach (var (id, subscriber) in _subscribers)
        {
            Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Info, "WaitAllTablesAreInitialized", $"Waiting for table [{id}] is initialized....", null));
            Console.WriteLine($"Waiting for table [{id}] being initialized....");
            await subscriber.IsInitialized();
            Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Info, "WaitAllTablesAreInitialized", $"Table [{id}] is initialized!!!!!", null));
            Console.WriteLine($"Table [{id}] is initialized!!!!!");
            
        }
    }
    
    protected void HandleIncomingPacket(SyncContract syncContract)
    {
        if (_subscribers.TryGetValue(syncContract.TableName, out var subscriber))
        {
            subscriber.UpdateData(syncContract);
        }
        else
        {
            Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Error, "HandleIncomingPacket", $"Somehow we are having packet for the table [{syncContract.TableName}] which we are not subscribed", null));
        }
    }

    protected abstract IInitTableSyncEvents<TDbRow> CreateInitTableSyncEvents<TDbRow>()
        where TDbRow : IMyNoSqlEntity, new();

    
    public MyNoSqlDataReader<TDbRow> Subscribe<TDbRow>(string tableName) where TDbRow: IMyNoSqlEntity, new()
    {
        var result = new MyNoSqlDataReader<TDbRow>(tableName, CreateInitTableSyncEvents<TDbRow>());
        _subscribers.Add(tableName, result);
        return result;
    }
}