using MyNoSqlDataReader.Core.Db;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.SyncEvents;


public class InitTableSyncEvent<TDbRow> where TDbRow : IMyNoSqlDbEntity, new()
{
    public SortedDictionary<String, DbPartition<TDbRow>> Partitions { get; }

    public InitTableSyncEvent(SortedDictionary<String, DbPartition<TDbRow>> partitions)
    {
        Partitions = partitions;
    }
}

public class InitPartitionSyncEvent<TDbRow> where TDbRow : IMyNoSqlDbEntity, new()
{
    public Dictionary<string, DbPartition<TDbRow>> UpdatedPartitions { get; }
    public InitPartitionSyncEvent(Dictionary<string, DbPartition<TDbRow>> updatedPartitions)
    {
        UpdatedPartitions = updatedPartitions;
    }

}

public class UpdateRowsSyncEvent<TDbRow> where TDbRow : IMyNoSqlDbEntity, new()
{
    public List<TDbRow> ChangedRows { get; }
    public UpdateRowsSyncEvent(List<TDbRow> changedRows)
    {
        ChangedRows = changedRows;
    }

}

public class DeleteRowsSyncEvent
{
    public Dictionary<string, List<string>> DeletedRows { get; }

    public DeleteRowsSyncEvent(Dictionary<string, List<string>> deletedRows)
    {
        DeletedRows = deletedRows;
    }
}

public interface IInitTableSyncEvents<TDbRow> where TDbRow : IMyNoSqlDbEntity, new()
{
    InitTableSyncEvent<TDbRow> ParseInitTable(SyncContract syncContract);
    InitPartitionSyncEvent<TDbRow> ParseInitPartitions(SyncContract syncContract);
    UpdateRowsSyncEvent<TDbRow> ParseUpdateRows(SyncContract syncContract);
    DeleteRowsSyncEvent ParseDeleteRows(SyncContract syncContract);
}