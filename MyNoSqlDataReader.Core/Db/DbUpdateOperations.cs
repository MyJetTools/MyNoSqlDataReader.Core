using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.Db;

public static class DbUpdateOperations
{
    public static void UpdateRows<TDbRow>(SortedDictionary<string, DbPartition<TDbRow>> tablePartitions,
        List<TDbRow> updatedRows) where TDbRow : IMyNoSqlEntity, new()
    {
        foreach (var dbRow in updatedRows)
        {
            if (!tablePartitions.ContainsKey(dbRow.PartitionKey))
            {
                tablePartitions.Add(dbRow.PartitionKey, new DbPartition<TDbRow>(dbRow.PartitionKey));
            }

            tablePartitions[dbRow.PartitionKey].InsertOrReplace(dbRow);
        }
    }

    public static void DeleteRows<TDbRow>(SortedDictionary<string, DbPartition<TDbRow>> tablePartitions,
        Dictionary<string, List<string>> deletedRows, Action<RowsUpdates<TDbRow>>? callback) where TDbRow : IMyNoSqlEntity, new()
    {

        List<TDbRow>? deletedRowsToCallback = null; 

        foreach (var (partitionKey, rowKeys) in deletedRows)
        {
            if (tablePartitions.TryGetValue(partitionKey, out var dbPartition))
            {

                foreach (var rowKey in rowKeys)
                {
                    var deletedRow = dbPartition.DeleteRow(rowKey);
                    
                    if (callback == null) continue;
                    if (deletedRow == null) continue;
                    deletedRowsToCallback ??= new List<TDbRow>();
                    deletedRowsToCallback.Add(deletedRow);
                }
            }
        }


        if (callback == null || deletedRowsToCallback == null) return;
        var difference = RowsUpdates<TDbRow>.CreateAsDeleted(deletedRowsToCallback);
        callback.Invoke(difference);

    }
    
    public static void PartitionsUpdate<TDbRow>(SortedDictionary<string, DbPartition<TDbRow>> tableDictionary, 
        IReadOnlyDictionary<string, DbPartition<TDbRow>> newPartitionsSet, Action<RowsUpdates<TDbRow>>? callback) where TDbRow : IMyNoSqlEntity, new()
    {

        foreach (var (partitionKey, newPartition) in newPartitionsSet)
        {
            if (tableDictionary.TryGetValue(partitionKey, out var tablePartition))
            {
                if (callback != null)
                {
                    var difference = tablePartition.GetAllDbRows().CalculateDifference(newPartition.GetAllDbRows());
                    callback.Invoke(difference);
                }
                tableDictionary.Remove(partitionKey);
            }
            else
            {
                if (callback != null)
                {
                    var difference = RowsUpdates<TDbRow>.CreateAsUpdated(newPartition.GetAllDbRows().Values);
                    callback.Invoke(difference);
                }
            }
                
            tableDictionary.Add(partitionKey, newPartition);
        }
    }

    public static void InitTable<TDbRow>(SortedDictionary<string, DbPartition<TDbRow>> tableDictionary,
        IReadOnlyDictionary<string, DbPartition<TDbRow>> newPartitionsSet, Action<RowsUpdates<TDbRow>>? callback)
        where TDbRow : IMyNoSqlEntity, new()
    {

        PartitionsUpdate(tableDictionary, newPartitionsSet, callback);
        
        foreach (var partitionKey in tableDictionary.Keys.ToList())
        {
            if (!newPartitionsSet.ContainsKey(partitionKey))
            {
                if (tableDictionary.Remove(partitionKey, out var removed))
                {
                    if (callback != null)
                    {
                        var difference = RowsUpdates<TDbRow>.CreateAsDeleted(removed.GetAllDbRows().Values);
                        callback.Invoke(difference);
                    }
                }
            }
        }
    }

}