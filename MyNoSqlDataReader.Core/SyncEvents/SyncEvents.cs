using MyNoSqlDataReader.Core.Db;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.SyncEvents;


public class InitTableSyncEvent<TDbRow>  where TDbRow: IMyNoSqlEntity, new()
{
    public SortedDictionary<String, DbPartition<TDbRow>> Partitions { get;  }
    
    private InitTableSyncEvent(SortedDictionary<String, DbPartition<TDbRow>> partitions)
    {
        Partitions = partitions;
    }
}

public class InitPartitionSyncEvent<TDbRow>  where TDbRow: IMyNoSqlEntity, new()
{
    public Dictionary<string, DbPartition<TDbRow>> UpdatedPartitions { get; }
    
    private InitPartitionSyncEvent(Dictionary<string, DbPartition<TDbRow>> updatedPartitions)
    {
        UpdatedPartitions = updatedPartitions;
    }
    
}

public class UpdateRowsSyncEvent<TDbRow>  where TDbRow: IMyNoSqlEntity, new()
{
    public List<TDbRow> ChangedRows { get;  }
    
    private UpdateRowsSyncEvent(List<TDbRow> changedRows)
    {
        ChangedRows = changedRows;
    }
 
}

public class DeleteRowsSyncEvent<TDbRow> where TDbRow: IMyNoSqlEntity, new()
{
    public Dictionary<string, List<string>> DeletedRows { get;  }
    
    private DeleteRowsSyncEvent(Dictionary<string, List<string>> deletedRows)
    {
        DeletedRows = deletedRows;
    }
}

public interface IInitTableSyncEvents<TDbRow> where TDbRow : IMyNoSqlEntity, new()
{
    InitTableSyncEvent<TDbRow> ParseInitTable(SyncContract syncContract);
    InitPartitionSyncEvent<TDbRow> ParseInitPartitions(SyncContract syncContract);
    UpdateRowsSyncEvent<TDbRow> ParseUpdateRows(SyncContract syncContract);
    DeleteRowsSyncEvent<TDbRow> ParseDeleteRows(SyncContract syncContract);
}