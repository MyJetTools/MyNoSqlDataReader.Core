using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.Core.Db;


public interface IDbTableReadAccess<TDbRow> where TDbRow: IMyNoSqlDbEntity, new()
{
    IReadOnlyDictionary<String, DbPartition<TDbRow>> GetReadAccess();
}

public interface IDbTableWriteAccess<TDbRow> where TDbRow: IMyNoSqlDbEntity, new()
{
    SortedDictionary<String, DbPartition<TDbRow>> GetWriteAccess();
}

public class DbTable<TDbRow> :  IDbTableReadAccess<TDbRow>, IDbTableWriteAccess<TDbRow> where TDbRow: IMyNoSqlDbEntity, new()
{
    public string Name { get; }

    private readonly SortedDictionary<String, DbPartition<TDbRow>> _partitions =
        new ();

    private readonly ReaderWriterLockSlim _rwLock = new ();

    public DbTable(string name)
    {
        Name = name;
    }
    
    public T GetReadAccess<T>(Func<IDbTableReadAccess<TDbRow>, T> readAccess)
    {
        _rwLock.EnterReadLock();
        try
        {
            return readAccess(this);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }
    
    public T? GetReadAccessWithNullableResult<T>(Func<IDbTableReadAccess<TDbRow>, T?> readAccess)
    {
        _rwLock.EnterReadLock();
        try
        {
            return readAccess(this);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }
    
    
    public void GetWriteAccess(Action<IDbTableWriteAccess<TDbRow>> writeAccess)
    {
        _rwLock.EnterWriteLock();
        try
        {
            writeAccess(this);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    IReadOnlyDictionary<string, DbPartition<TDbRow>> IDbTableReadAccess<TDbRow>.GetReadAccess()
    {
        return _partitions;
    }
    
    SortedDictionary<string, DbPartition<TDbRow>> IDbTableWriteAccess<TDbRow>.GetWriteAccess()
    {
        return _partitions;
    }

}