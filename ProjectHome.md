A small C# library with transaction support to be used with MySQL Connector .NET for simple database communication

The multicon object is for open-then-close querying. Onecon object is used when dealing with transactions, memory tables and when you don't want to reconnect between queries.

**See Wiki for more examples**

Transaction example:
```
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.MysqlHelper;
using MySql.MysqlHelper.Misc;

class Program
{
    static void Main(string[] args)
    {
        using (OneCon oneCon = new OneCon(new ConnectionString("localhost", "login", "psw", 3306)))
        {
            oneCon.InsertRow("various", "innodb_table", false, new ColumnData("varchar_column", "my data row 1"));

            oneCon.Commit(respring: true); // Performs changes. Respring is true since we'll make more queries after this

            oneCon.InsertRow("various", "innodb_table", false, new ColumnData("varchar_column", "my data row 2"));

            oneCon.Rollback(respring: true); // Nope. I changed my mind. Don't perform previous changes. More queries to come

            oneCon.InsertRow("various", "innodb_table", false, new ColumnData("varchar_column", "my data row 3"));

            oneCon.Commit(respring: false); // Performs changes. No more transactions after this.
        }
    }
}
```


Simple non-transaction querying:
```
using System;
using System.Collections.Generic;
using System.Linq;
using MySql.MysqlHelper;

namespace ConsoleApplication
{
    class Program
    {
        private static MultiCon multiCon = new MultiCon(new ConnectionString("my.mysqldb.com", "username", "password"));

        static void Main(string[] args)
        {
            DateTime currentTimestamp = multiCon.GetObject<DateTime>("SELECT CURRENT_TIMESTAMP");
            List<string> databases = multiCon.GetColumn<string>("SHOW DATABASES", 0, false).ToList();
        }
    }
}
```