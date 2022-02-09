using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.Db;

public class DbPartition<TDbRow> where TDbRow : IMyNoSqlEntity, new()
{
    public string PartitionKey { get; }
    
    private readonly SortedDictionary<string, TDbRow> _dbRows = new ();

    public DbPartition(string partitionKey)
    {
        PartitionKey = partitionKey;
    }

    public IReadOnlyDictionary<string, TDbRow> GetAllDbRows()
    {
        return _dbRows;
    }

    public TDbRow? TryGetRow(string rowKey)
    {
        return _dbRows.TryGetValue(rowKey, out var result) ? result : default;
    }

    public TDbRow? InsertOrReplace(TDbRow dbRow)
    {
        if (_dbRows.Remove(dbRow.RowKey, out var removed))
        {
            _dbRows.Add(dbRow.RowKey, dbRow);
            return removed;
        }
        
        _dbRows.Add(dbRow.RowKey, dbRow);
        return default;
    }
    
    public void BulkInsertOrReplace(IEnumerable<TDbRow> dbRows)
    {
        foreach (var dbRow in dbRows)
        {
            InsertOrReplace(dbRow);
        }
    }

    public TDbRow? DeleteRow(string rowKey)
    {
        return _dbRows.Remove(rowKey, out var result) ? result : default;
    }

    public int Count => _dbRows.Count;
}