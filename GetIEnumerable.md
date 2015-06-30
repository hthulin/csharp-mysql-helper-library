# Introduction #

Example of using GetIEnumerable`<T>` to populate a list with instances

GetIEnumerableParse`<T>` parses database values as string

# Details #

```
using System;
using System.Collections.Generic;
using System.Linq;
using MySql.MysqlHelper;

namespace ConsoleApplication
{
    class Program
    {
        public class InfoSchema
        {
            public string table_schema { get; set; }
            public string table_name { get; set; }
        }

        static void Main(string[] args)
        {
            MultiCon multiCon = new MySql.MysqlHelper.MultiCon(new ConnectionString("my.mysqldb.com", "username", "password"));

            List<InfoSchema> myDatabaseAndTables = multiCon.GetIEnumerable<InfoSchema>("SELECT `TABLE_SCHEMA` , `TABLE_NAME` FROM `information_schema`.`TABLES`").ToList();

            myDatabaseAndTables.ForEach(n => System.Diagnostics.Debug.WriteLine(string.Format("Database: {0}, Table: {1}", n.table_schema, n.table_name)));
        }
    }
}

```