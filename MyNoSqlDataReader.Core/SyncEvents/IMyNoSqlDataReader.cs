namespace MyNoSqlDataReader.Core.SyncEvents;

public interface IMyNoSqlDataReader
{
    void UpdateData(ISyncEvent syncContract);
}