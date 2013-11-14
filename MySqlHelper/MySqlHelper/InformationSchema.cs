using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    public class InformationSchema
    {
        public class TableUpdates
        {
            private readonly object _lock = new object();

            private Dictionary<Tuple<string, string>, DateTime> dictUpdates = new Dictionary<Tuple<string, string>, DateTime>();

            private MultiCon multiCon = null;

            public TableUpdates(ConnectionString options)
            {
                multiCon = new MultiCon(options);
            }

            public bool HasChanged(string database, string table)
            {
                Tuple<string, string> identifier = new Tuple<string, string>(database, table);

                string query = "SELECT `UPDATE_TIME` FROM `information_schema`.`TABLES`  WHERE `TABLE_SCHEMA`='" + database + "' && `TABLE_NAME`='" + table + "' LIMIT 1;";

                DateTime lastModified = multiCon.GetObject<DateTime>(query);

                lock (_lock)
                {
                    if (!dictUpdates.ContainsKey(identifier))
                        dictUpdates.Add(identifier, lastModified);
                    else
                        if (dictUpdates[identifier] == lastModified)
                            return false;
                        else
                            dictUpdates[identifier] = lastModified;
                }

                return true;
            }

        }
    }
}
