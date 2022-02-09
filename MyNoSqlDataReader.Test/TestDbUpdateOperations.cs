using System;
using System.Collections.Generic;
using MyNoSqlDataReader.Core.Db;
using NUnit.Framework;

namespace MyNoSqlDataReader.Test;


public class TestDbUpdateOperations
{
    [Test]
    public void TestUpdateRowsToEmptySource()
    {
        var source = new Dictionary<string, DbPartition<TestEntity>>();

        var timeStamp = DateTime.UtcNow;

        var updatedRows = new List<TestEntity>
        {
            new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = timeStamp
            },

            new ()
            {
                PartitionKey = "PK",
                RowKey = "RK2",
                TimeStamp = timeStamp
            },

        };

        DbUpdateOperations.UpdateRows(source, updatedRows);
        Assert.AreEqual(1, source.Count);
        Assert.AreEqual(true, source.ContainsKey("PK"));


        Assert.AreEqual(timeStamp, source["PK"].TryGetRow("RK1").TimeStamp);
        Assert.AreEqual(timeStamp, source["PK"].TryGetRow("RK2").TimeStamp);
    }


    [Test]
    public void TestUpdateRowsWithExistingRecords()
    {

        var dbPartition = new DbPartition<TestEntity>("PK");
        var timeStamp1 = DateTime.UtcNow;
        var timeStamp2 = timeStamp1.AddSeconds(1);

        dbPartition.InsertOrReplace(new()
        {
            PartitionKey = "PK",
            RowKey = "RK",
            TimeStamp = timeStamp1
        });

        var source = new Dictionary<string, DbPartition<TestEntity>>
        {
            ["PK"] = dbPartition
        };

        var updatedRows = new List<TestEntity>
        {
            new()
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = timeStamp2
            }
        };

        DbUpdateOperations.UpdateRows(source, updatedRows);
        Assert.AreEqual(1, source.Count);
        Assert.AreEqual(true, source.ContainsKey("PK"));

        Assert.AreEqual(timeStamp2, source["PK"].TryGetRow("RK").TimeStamp);
    }

    [Test]
    public void TestSkippingObsoleteUpdateRowsWithExistingRecords()
    {

        var dbPartition = new DbPartition<TestEntity>("PK");
        var timeStamp1 = DateTime.UtcNow;
        var timeStamp2 = timeStamp1.AddSeconds(-1); //Let's assume somehow we got an old record to update

        dbPartition.InsertOrReplace(new()
        {
            PartitionKey = "PK",
            RowKey = "RK",
            TimeStamp = timeStamp1
        });

        var source = new Dictionary<string, DbPartition<TestEntity>>
        {
            ["PK"] = dbPartition
        };

        var updatedRows = new List<TestEntity>
        {
            new()
            {
                PartitionKey = "PK",
                RowKey = "RK",
                TimeStamp = timeStamp2
            }
        };

        DbUpdateOperations.UpdateRows(source, updatedRows);
        Assert.AreEqual(1, source.Count);
        Assert.AreEqual(true, source.ContainsKey("PK"));

        Assert.AreEqual(timeStamp1, source["PK"].TryGetRow("RK").TimeStamp);
    }


    [Test]
    public void TestDeleteRows()
    {
        var source = new Dictionary<string, DbPartition<TestEntity>>();


        var timeStamp = DateTime.UtcNow;

        var dbPartition = new DbPartition<TestEntity>("PK");


        dbPartition.InsertOrReplace(new TestEntity
        {
            PartitionKey = "PK",
            RowKey = "RK1",
            TimeStamp = timeStamp
        });

        dbPartition.InsertOrReplace(new TestEntity
        {
            PartitionKey = "PK",
            RowKey = "RK2",
            TimeStamp = timeStamp
        });

        source.Add("PK", dbPartition);

        var rowsToDelete = new Dictionary<string, List<string>>
        {
            ["PK"] = new() { "RK2" }
        };

        DbUpdateOperations.DeleteRows(source, rowsToDelete, null);
        Assert.AreEqual(1, source.Count);
        Assert.AreEqual(true, source.ContainsKey("PK"));

        Assert.IsNull(source["PK"].TryGetRow("RK2"));
        Assert.AreEqual(timeStamp, source["PK"].TryGetRow("RK1").TimeStamp);
    }


}
