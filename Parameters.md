# Introduction #

Example of using parameters. Loads database name and table name information into a datatable where MyISAM engine is being used, and displays them in the debug output window.


# Details #

```
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using MySql.MysqlHelper;

namespace ConsoleApplication
{
    class Program
    {
        private static MultiCon multiCon = new MultiCon(new ConnectionString("my.mysqldb.com", "username", "password"));

        static void Main(string[] args)
        {
            ColumnData where = new ColumnData("ENGINE", "MyISAM");
            using (DataTable table = multiCon.GetDataTable("SELECT`TABLE_SCHEMA`,`TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `ENGINE`=@ENGINE", where))
            {
                foreach (DataRow row in table.Rows)
                {
                    Debug.WriteLine("{0}.{1} is MyISAM", row.Field<string>("TABLE_SCHEMA"), row.Field<string>("TABLE_NAME"));
                }
            }
        }
    }
}

```