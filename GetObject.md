# Introduction #

GetObject returns cell value as a object

GetObject`<T>` Returns cell value from database. Column type must match variable type

GetObjectParse`<T>` parses the value as a string

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
    static void Main(string[] args)
    {
        MultiCon multiCon = new MultiCon(new ConnectionString("localhost", "login", "psw", 3306));

        DateTime timeDateTime = multiCon.GetObject<DateTime>("SELECT CURRENT_TIMESTAMP");

        string timeString = multiCon.GetObjectParse<string>("SELECT CURRENT_TIMESTAMP");

        System.Diagnostics.Debug.WriteLine("Server time DateTime is {0}", timeDateTime);
        System.Diagnostics.Debug.WriteLine("Server time ToString is " + timeString);
    }
}
```