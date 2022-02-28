namespace MyNoSqlDataReader.Core.SyncEvents;

public interface IMyNoSqlDataReaderEventUpdater
{
    void UpdateData(SyncContract syncContract);
}