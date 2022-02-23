using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.Db;


public class RowsUpdates<TDbRow> where TDbRow : IMyNoSqlEntity, new()
{
    public List<TDbRow>? Updated { get; private set; }
    public List<TDbRow>? Deleted { get; private set; }

    internal void AddUpdated(TDbRow dbRow)
    {
        Updated ??= new();
        Updated.Add(dbRow);
    }
    
    internal void AddDeleted(TDbRow dbRow)
    {
        Deleted ??= new();
        Deleted.Add(dbRow);
    }
    
    public static RowsUpdates<TDbRow>  CreateAsUpdated(IEnumerable<TDbRow> dbRows)
    {
        return new RowsUpdates<TDbRow>
        {
            Updated = dbRows.ToList()
        };
    }
    
    public static RowsUpdates<TDbRow>  CreateAsDeleted(IEnumerable<TDbRow> dbRows)
    {
        return new RowsUpdates<TDbRow>
        {
            Deleted = dbRows.ToList()
        };
    }

}

public static class DbPartitionDifferenceCalculator
{
    public static RowsUpdates<TDbRow> CalculateDifference<TDbRow>(this IReadOnlyDictionary<string, TDbRow> before, 
        IReadOnlyDictionary<string, TDbRow> after) where TDbRow : IMyNoSqlEntity, new()
    {

        var result = new RowsUpdates<TDbRow>();

        foreach (var beforeRow in before.Values)
        {
            if (after.TryGetValue(beforeRow.RowKey, out var afterRow))
            {
                if (afterRow.TimeStamp != beforeRow.TimeStamp)
                {
                    result.AddUpdated(afterRow);
                }
            }
            else
            {
                result.AddDeleted(beforeRow);
            }
        }

        foreach (var afterRow in after.Values)
        {
            if (!before.ContainsKey(afterRow.RowKey))
            {
                result.AddUpdated(afterRow);
            }
        }

        return result;
    }
    
}