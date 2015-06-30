# Introduction #

Simple example of using attributes for generic functions


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
    private static MultiCon multicon = new MultiCon(new MySql.MysqlHelper.ConnectionString("mydb.com", "user", "psw"));

    public class TestClass
    {
        [MySqlColumn(DatabaseColumnName = "table_schema")] // Read and write fields will always be true if not defined
        public string Database { get; set; }

        [MySqlColumn(DatabaseColumnName = "table_name", ReadFromDatabase = true, WriteToDatabase = false)] // No writing here since it's a read-only table
        public string Table { get; set; }

        public string DatabaseTableQuery // Will be ignored since no MySqlTable attribute has been set
        {
            get
            {
                return string.Format("`{0}`.`{1}`", Database, Table);
            }
        }
    }

    static void Main(string[] args)
    {
        List<TestClass> t = multicon.GetIEnumerable<TestClass>("SELECT `" + (string.Join("`,`", MysqlTableAttributeFunctions.GetReadColumnNames(typeof(TestClass)))) + "` FROM `information_schema`.`TABLES` LIMIT 10;").ToList();

        t.ForEach(n => System.Diagnostics.Debug.WriteLine("Query string: " + n.DatabaseTableQuery));
    }
}
```