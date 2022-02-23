using MyNoSqlDataReader.Core.Db;
using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core;

public class MyNoSqlDataReader<TDbRow> : IMyNoSqlDataReader where TDbRow: IMyNoSqlDbEntity, new()
{
    private readonly IInitTableSyncEvents<TDbRow> _initTableSyncEvents;
    private readonly DbTable<TDbRow> _dbTable;

    private bool _setInitialized;
    private readonly TaskCompletionSource _initialized = new ();
    public Task AwaitInitialized => _initialized.Task;

    public MyNoSqlDataReader(string name, IInitTableSyncEvents<TDbRow> initTableSyncEvents)
    {
        _initTableSyncEvents = initTableSyncEvents;
        _dbTable =  new DbTable<TDbRow>(name);
    }

    void IMyNoSqlDataReader.UpdateData(SyncContract syncContract)
    {
        switch (syncContract.SyncEventType)
        {
            case SyncEventType.InitTable:
                if (!_setInitialized)
                {
                    _setInitialized = true;
                    _initialized.SetResult();
                    Console.WriteLine($"DataReader Table {syncContract.TableName} is initialized");
                }
                HandleInitTable(_initTableSyncEvents.ParseInitTable(syncContract));
                return;
            
            case SyncEventType.InitPartitions:
                HandleInitPartition(_initTableSyncEvents.ParseInitPartitions(syncContract));
                return;
            
            case SyncEventType.UpdateRows:
                HandleUpdateRows(_initTableSyncEvents.ParseUpdateRows(syncContract));
                return;
            
            case SyncEventType.DeleteRows:
                HandleDeleteRows(_initTableSyncEvents.ParseDeleteRows(syncContract));
                return;
        }

        throw new Exception($"Unknown event type {syncContract.SyncEventType}");
    }


    private Action<RowsUpdates<TDbRow>>? _callback;

    public void RegisterRowsUpdatesCallback(Action<RowsUpdates<TDbRow>> callback)
    {
        _callback = callback;
    }

    private void HandleInitTable(InitTableSyncEvent<TDbRow> initTableSyncEvent)
    {

        _dbTable.GetWriteAccess(writeAccess =>
        {
            var tableDictionary = writeAccess.GetWriteAccess();
            DbUpdateOperations.InitTable(tableDictionary, initTableSyncEvent.Partitions, _callback);
        });
    }
    
    private void HandleInitPartition(InitPartitionSyncEvent<TDbRow> initPartitionsToSync)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            var tableDictionary = writeAccess.GetWriteAccess();
            DbUpdateOperations.PartitionsUpdate(tableDictionary, initPartitionsToSync.UpdatedPartitions, _callback);
        });
    }
    
    
    private void HandleUpdateRows(UpdateRowsSyncEvent<TDbRow> updateRowsSyncEvent)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            DbUpdateOperations.UpdateRows(writeAccess.GetWriteAccess(), updateRowsSyncEvent.ChangedRows);
            
            if (_callback != null)
            {
                var difference = RowsUpdates<TDbRow>.CreateAsUpdated(updateRowsSyncEvent.ChangedRows);
                _callback?.Invoke(difference);
            }
        });
    }    
    
    private void HandleDeleteRows(DeleteRowsSyncEvent deleteRowsSyncEvent)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            DbUpdateOperations.DeleteRows(writeAccess.GetWriteAccess(), deleteRowsSyncEvent.DeletedRows, _callback);
        });
    }

    public int Count
    {
        get
        {
            return _dbTable.GetReadAccess(readAccess =>
            {
                return readAccess.GetReadAccess().Values.Sum(dbPartition => dbPartition.Count);
            });
        }
    }

    public int? TryGetRecordsCount(string partitionKey)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess =>
        {
            if (!readAccess.GetReadAccess().TryGetValue(partitionKey, out var partition)) 
                return null;
            
            int? result = partition.Count;
            return result;

        });

    }


    public int GetRecordsCount(string partitionKey)
    {
        return _dbTable.GetReadAccess(readAccess => !readAccess.GetReadAccess().TryGetValue(partitionKey, out var partition) ? 0 : partition.Count);

    }

    public TDbRow? TryGetRow(string partitionKey, string rowKey)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess => !readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition) ? default : dbPartition.TryGetRow(rowKey));
    }


    public IReadOnlyList<TDbRow>? TryGetRows(string partitionKey)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess =>
        {
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
                return dbPartition.GetAllDbRows().Values.ToList();
            return null;

        });
    }
    
    public IReadOnlyList<TDbRow>? TryGetRows(string partitionKey, Func<TDbRow, bool> filter)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess =>
        {
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
                return dbPartition.GetAllDbRows().Values.Where(filter).ToList();
            return null;

        });
    }
    
    
    public IReadOnlyList<TDbRow> GetRows()
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();

            foreach (var dbRows in readAccess.GetReadAccess().Values)
            {
                result.AddRange(dbRows.GetAllDbRows().Values);
            }

            return result;
        });
    }
    
    public IReadOnlyList<TDbRow> GetRows(Func<TDbRow, bool> filter)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();

            foreach (var dbRows in readAccess.GetReadAccess().Values)
            {
                result.AddRange(dbRows.GetAllDbRows().Values.Where(filter));
            }

            return result;
        });
    }

}