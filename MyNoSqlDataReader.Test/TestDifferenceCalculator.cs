using System;
using System.Collections.Generic;
using MyNoSqlDataReader.Core.Db;
using MyNoSqlServer.Abstractions;
using NUnit.Framework;

namespace MyNoSqlDataReader.Test;

public class TestEntity : MyNoSqlEntity
{

}

public class TestDifferenceCalculator
{

    [Test]
    public void TestNewRecords()
    {

        var before = new SortedDictionary<string, TestEntity>();

        var after = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = DateTime.UtcNow
            }
        };

        var difference = before.CalculateDifference(after);

        Assert.IsNull(difference.Deleted);
        Assert.IsNotNull(difference.Updated);
        Assert.AreEqual(1, difference.Updated.Count);
    }

    [Test]
    public void TestDeletedRecords()
    {

        var before = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = DateTime.UtcNow
            }
        };

        var after = new SortedDictionary<string, TestEntity>();
        ;

        var difference = before.CalculateDifference(after);

        Assert.IsNull(difference.Updated);
        Assert.IsNotNull(difference.Deleted);
        Assert.AreEqual(1, difference.Deleted.Count);
    }

    [Test]
    public void TestNothingChanged()
    {

        var timeStamp = DateTime.UtcNow;
        var before = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = timeStamp
            }
        };

        var after = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = timeStamp
            }
        };
        ;

        var difference = before.CalculateDifference(after);

        Assert.IsNull(difference.Updated);
        Assert.IsNull(difference.Deleted);
    }


    [Test]
    public void TestRecordUpdated()
    {

        var beforeTimeStamp = DateTime.UtcNow;
        var afterTimeStamp = beforeTimeStamp.AddDays(1);
        var before = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = beforeTimeStamp
            }
        };

        var after = new SortedDictionary<string, TestEntity>
        {
            ["RK1"] = new()
            {
                PartitionKey = "PK",
                RowKey = "RK1",
                TimeStamp = afterTimeStamp
            }
        };
        ;

        var difference = before.CalculateDifference(after);

        Assert.IsNull(difference.Deleted);
        Assert.IsNotNull(difference.Updated);
        Assert.AreEqual(1, difference.Updated.Count);
        Assert.AreEqual(afterTimeStamp, difference.Updated[0].TimeStamp);
    }

}