
using MyNoSqlDataReader.Core.Db;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.SyncEvents;


public interface ISyncEvent
{
    
}

public class InitTableSyncEvent<TDbRow> : ISyncEvent  where TDbRow: IMyNoSqlEntity, new()
{
    public string TableName { get;  }
    public Dictionary<string, DbPartition<TDbRow>> Records { get; }
    public InitTableSyncEvent(string tableName, Dictionary<string, DbPartition<TDbRow>> records )
    {
        TableName = tableName;
        Records = records;
    }
}

public class InitPartitionsSyncEvent<TDbRow> : ISyncEvent where TDbRow: IMyNoSqlEntity, new()
{
    public string TableName { get;  }
    public Dictionary<string, DbPartition<TDbRow>> Records { get; }
    public InitPartitionsSyncEvent(string tableName, Dictionary<string, DbPartition<TDbRow>> records )
    {
        TableName = tableName;
        Records = records;
    }
}

public class UpdateRowsSyncEvent<TDbRow> : ISyncEvent where TDbRow: IMyNoSqlEntity, new()
{
    public string TableName { get;  }
    public List<TDbRow> UpdatedRows { get; }
    public UpdateRowsSyncEvent(string tableName, List<TDbRow> updatedRows)
    {
        TableName = tableName;
        UpdatedRows = updatedRows;
    }
}


public class DeleteRowsSyncEvent : ISyncEvent
{
    public string TableName { get;  }
    public Dictionary<string, List<string>> DeletedRows { get; }
    public DeleteRowsSyncEvent(string tableName, Dictionary<string, List<string>> deletedRows)
    {
        TableName = tableName;
        DeletedRows = deletedRows;
    }
}



