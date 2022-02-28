using MyNoSqlDataReader.Core.Db;
using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core;

public class MyNoSqlDataReader<TDbRow> : IMyNoSqlDataReaderEventUpdater, IMyNoSqlServerDataReader<TDbRow> where TDbRow: IMyNoSqlEntity, new()
{
    private readonly IInitTableSyncEvents<TDbRow> _initTableSyncEvents;
    private readonly DbTable<TDbRow> _dbTable;

    private bool _setInitialized;
    private readonly TaskCompletionSource _initialized = new ();

    public MyNoSqlDataReader(string name, IInitTableSyncEvents<TDbRow> initTableSyncEvents)
    {
        _initTableSyncEvents = initTableSyncEvents;
        _dbTable =  new DbTable<TDbRow>(name);
    }

    void IMyNoSqlDataReaderEventUpdater.UpdateData(SyncContract syncContract)
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


    private Action<RowsUpdates<TDbRow>>? _theCallback;

    private bool HasSubscriber()
    {
        return _theCallback != null || _updateSubscriber != null || _deleteSubscriber != null;
    }

    private void InvokeCallback(RowsUpdates<TDbRow> rowsToUpdate)
    {
        _theCallback?.Invoke(rowsToUpdate);

        if (rowsToUpdate.Updated != null)
            _updateSubscriber?.Invoke(rowsToUpdate.Updated);
        
        if (rowsToUpdate.Deleted != null)
            _deleteSubscriber?.Invoke(rowsToUpdate.Deleted);
    }

    public void RegisterRowsUpdatesCallback(Action<RowsUpdates<TDbRow>> callback)
    {
        _theCallback = callback;
    }

    private void HandleInitTable(InitTableSyncEvent<TDbRow> initTableSyncEvent)
    {

        _dbTable.GetWriteAccess(writeAccess =>
        {
            var tableDictionary = writeAccess.GetWriteAccess();
            DbUpdateOperations.InitTable(tableDictionary, initTableSyncEvent.Partitions, InvokeCallback);
        });
    }
    
    private void HandleInitPartition(InitPartitionSyncEvent<TDbRow> initPartitionsToSync)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            var tableDictionary = writeAccess.GetWriteAccess();
            DbUpdateOperations.PartitionsUpdate(tableDictionary, initPartitionsToSync.UpdatedPartitions, InvokeCallback);
        });
    }
    
    
    private void HandleUpdateRows(UpdateRowsSyncEvent<TDbRow> updateRowsSyncEvent)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            DbUpdateOperations.UpdateRows(writeAccess.GetWriteAccess(), updateRowsSyncEvent.ChangedRows);
            
            if (HasSubscriber())
            {
                var difference = RowsUpdates<TDbRow>.CreateAsUpdated(updateRowsSyncEvent.ChangedRows);
                InvokeCallback(difference);
            }
        });
    }    
    
    private void HandleDeleteRows(DeleteRowsSyncEvent deleteRowsSyncEvent)
    {
        _dbTable.GetWriteAccess(writeAccess =>
        {
            DbUpdateOperations.DeleteRows(writeAccess.GetWriteAccess(), deleteRowsSyncEvent.DeletedRows, InvokeCallback);
        });
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
    

    public IReadOnlyList<TDbRow>? TryGetRows(string partitionKey)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess =>
        {
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
                return dbPartition.GetAllDbRows().Values.ToList();
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

    public Task IsInitialized()
    {
        return _initialized.Task;
    }

    public TDbRow? Get(string partitionKey, string rowKey)
    {
        return _dbTable.GetReadAccessWithNullableResult(readAccess => !readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition) ? default : dbPartition.TryGetRow(rowKey));
    }

    public IReadOnlyList<TDbRow> Get(string partitionKey)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
                return dbPartition.GetAllDbRows().Values.ToList() as IReadOnlyList<TDbRow>;
            return Array.Empty<TDbRow>();

        });
    }

    public IReadOnlyList<TDbRow> Get(string partitionKey, int skip, int take)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
            {
                result.AddRange(dbPartition.GetAllDbRows().Values.Skip(skip).Take(take));  
            }
            return result;
        });
    }

    public IReadOnlyList<TDbRow> Get(string partitionKey, int skip, int take, Func<TDbRow, bool> condition)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
            {
                result.AddRange(dbPartition.GetAllDbRows().Values.Where(condition).Skip(skip).Take(take));  
            }
            return result;
        });
    }

    public IReadOnlyList<TDbRow> Get(string partitionKey, Func<TDbRow, bool> condition)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();
            if (readAccess.GetReadAccess().TryGetValue(partitionKey, out var dbPartition))
            {
                result.AddRange(dbPartition.GetAllDbRows().Values.Where(condition));  
            }
            return result;
        });
    }

    public IReadOnlyList<TDbRow> Get(Func<TDbRow, bool>? condition = null)
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            var result = new List<TDbRow>();

            foreach (var dbRows in readAccess.GetReadAccess().Values)
            {
                result.AddRange(condition == null
                    ? dbRows.GetAllDbRows().Values
                    : dbRows.GetAllDbRows().Values.Where(condition));
            }

            return result;
        });
    }

    int IMyNoSqlServerDataReader<TDbRow>.Count()
    {
        return _dbTable.GetReadAccess(readAccess =>
        {
            return readAccess.GetReadAccess().Values.Sum(dbPartition => dbPartition.Count);
        });
    }

    int IMyNoSqlServerDataReader<TDbRow>.Count(string partitionKey)
    {
        return _dbTable.GetReadAccess(readAccess => !readAccess.GetReadAccess().TryGetValue(partitionKey, out var partition) ? 0 : partition.Count);
    }

    int IMyNoSqlServerDataReader<TDbRow>.Count(string partitionKey, Func<TDbRow, bool> condition)
    {
        return _dbTable.GetReadAccess(readAccess => readAccess.GetReadAccess().TryGetValue(partitionKey, out var partition) ? partition.GetAllDbRows().Values.Count(condition) : 0);
    }


    private Action<IReadOnlyList<TDbRow>>? _updateSubscriber;
    private Action<IReadOnlyList<TDbRow>>? _deleteSubscriber;
    
    
    public IMyNoSqlServerDataReader<TDbRow> SubscribeToUpdateEvents(Action<IReadOnlyList<TDbRow>> updateSubscriber, Action<IReadOnlyList<TDbRow>> deleteSubscriber)
    {
        _updateSubscriber = updateSubscriber;
        _deleteSubscriber = deleteSubscriber;
        return this;
    }
}