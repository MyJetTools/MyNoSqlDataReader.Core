
namespace MyNoSqlDataReader.Core.SyncEvents;

public enum SyncEventType
{
    InitTable, InitPartitions, UpdateRows, DeleteRows
}

public class SyncContract
{
    public SyncEventType SyncEventType { get;  }
    public string TableName { get;  }
    public ReadOnlyMemory<byte> Payload { get; }
    public SyncContract(string tableName, SyncEventType syncEventType, ReadOnlyMemory<byte> payload)
    {
        TableName = tableName;
        SyncEventType = syncEventType;
        Payload = payload;
    }
}


