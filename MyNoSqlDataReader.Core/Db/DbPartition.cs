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

    public void InsertOrReplace(TDbRow dbRow)
    {
        if (_dbRows.TryGetValue(dbRow.RowKey, out var current))
        {
            if (current.TimeStamp<dbRow.TimeStamp)
            {
                if (_dbRows.Remove(dbRow.RowKey))
                {
                    _dbRows.Add(dbRow.RowKey, dbRow);
                    return;
                }
            }
            else
            {
                return;
            }
        }
        
        _dbRows.Add(dbRow.RowKey, dbRow);
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