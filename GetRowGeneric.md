# Introduction #

Loads instance with database row

GetRowGenericParse`<T>` does basically the same while instead parse return values to instance property type as string



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
        MultiCon multiCon = new MultiCon(new MySql.MysqlHelper.ConnectionString("localhost", "login", "psw", 3306));

        TableData tableData = new TableData();

        string query = "SELECT `" + string.Join("`,`", MysqlTableAttributeFunctions.GetReadColumnNames(typeof(TableData))) + "` FROM `various`.`test` WHERE `idnum`=@id";

        multiCon.GetRowGenericParse<TableData>(query, tableData, new ColumnData("id", 1));
    }
}
```