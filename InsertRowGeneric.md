# Introduction #

InsertRowGeneric`<T>` example


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
        [MySqlColumn(DatabaseColumnName = "idnum", WriteToDatabase = false)] // Won't set auto increment column
        public uint id { get; set; }

        [MySqlColumn]
        public string column1 { get; set; }

        [MySqlColumn]
        public string column2 { get; set; }
    }

    static void Main(string[] args)
    {
        MultiCon multiCon = new MultiCon(new MySql.MysqlHelper.ConnectionString("localhost", "login", "password", 3306));

        TableData tableData = new TableData() { column1 = "abc", column2 = "def" };

        tableData.id = (uint)multiCon.InsertRowGeneric<TableData>("various", "test", false, tableData);
    }
}
```