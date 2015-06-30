# Introduction #

Example of populating a DataTable with a database table

# Details #

```
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.MysqlHelper;
using MySql.MysqlHelper.Misc;

class Program
{
    static void Main(string[] args)
    {
        MultiCon multiCon = new MultiCon(new ConnectionString("localhost", "login", "psw", 3306));

        string query = "SELECT `CHARACTER_SET_NAME` , `DEFAULT_COLLATE_NAME` , `DESCRIPTION` , `MAXLEN` FROM `information_schema`.`CHARACTER_SETS`";

        using (DataTable table = multiCon.GetDataTable(query))
            foreach (DataRow row in table.Rows)
            {
                System.Diagnostics.Debug.WriteLine(row.Field<string>("CHARACTER_SET_NAME"));
            }

    }
}
```