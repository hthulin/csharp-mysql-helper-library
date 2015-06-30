# Introduction #

Example of using GetIDictionary`<T>` to populate a dictionary with instances

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
    public class DatabaseInfo
    {
        public string character_set_name { get; set; }
        public string description { get; set; }
    }

    static void Main(string[] args)
    {
        MultiCon multiCon = new MultiCon(new ConnectionString("localhost", "login", "psw", 3306));

        string query = "SELECT `CHARACTER_SET_NAME` , `DESCRIPTION` FROM `information_schema`.`CHARACTER_SETS`";
        string keyColumn = "CHARACTER_SET_NAME";

        var dictionary = multiCon.GetIDictionary<string, DatabaseInfo>(keyColumn, query, parseKey: false).ToDictionary(k => k.Key, v => v.Value);

        System.Diagnostics.Debug.WriteLine(dictionary["utf8"].description);
    }
}
```