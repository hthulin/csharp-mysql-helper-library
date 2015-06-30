# Introduction #

Bulk upload of instance objects to database

# Details #

```
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.MysqlHelper;
using MySql.MysqlHelper.Misc;

class Program
{
    public class TableData
    {
        [MySqlColumn(DatabaseColumnName = "idnum", WriteToDatabase = false)] // Auto-increment column will not be sent to the database
        public uint id { get; set; }

        [MySqlColumn]
        public string column1 { get; set; }

        [MySqlColumn]
        public string column2 { get; set; }

        [MySqlColumn]
        public int column3 { get; set; }

        public string OrdinaryDataNotDatabaseRelated { get; set; } // No attribute. Won't be read nor written
    }

    static void Main(string[] args)
    {
        MultiCon multiCon = new MultiCon(new MySql.MysqlHelper.ConnectionString("localhost", "login", "psw", 3306));

        List<TableData> tableData = new List<TableData>();

        tableData.Add(new TableData() { column1 = "a1", column2 = "a2", column3 = 1 });
        tableData.Add(new TableData() { column1 = "b1", column2 = "b2", column3 = 2 });

        multiCon.BulkSendGeneric<TableData>("various", "test", tableData, onDuplicateUpdate: true, updateBatchSize: 100, continueUpdateOnError: true);
    }
}
```