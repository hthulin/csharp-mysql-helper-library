# Introduction #

Performing transactions with InnoDB tables

# On exception #

This example will rollback any changes if unable to perform all inserts

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
        string database = "various";
        string table = "innodb_table";

        using (OneCon oneCon = new OneCon(new ConnectionString("localhost", "login", "psw", 3306)))
        {
            try
            {
                oneCon.InsertRow(database, table, false, new ColumnData("varchar_column", "my data row 1"));
                oneCon.InsertRow(database, table, false, new ColumnData("varchar_column", "my data row 2"));
                oneCon.InsertRow(database, table, false, new ColumnData("varchar_column", "my data row 3"));
                oneCon.Commit(respring: false); // Performs inserts. No more transactions after this.
            }
            catch
            {
                oneCon.Rollback(); // Something happened. Do not perform any of the inserts
            }
        }
    }
}

```


# General #

The following example will insert row 1 and 3.

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

