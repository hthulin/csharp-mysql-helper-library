using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySql.MysqlHelper.Misc
{
    public class Status
    {
        private double differenceSeconds = double.NaN;

        public DateTime GetServerDateTime(MySql.MysqlHelper.MultiCon connection)
        {
            return GetServerDateTime<MySql.MysqlHelper.MultiCon>(connection);
        }

        public DateTime GetServerDateTime(MySql.MysqlHelper.OneCon connection)
        {
            return GetServerDateTime<MySql.MysqlHelper.OneCon>(connection);
        }

        private DateTime GetServerDateTime<T>(T connection) where T : Default
        {
            if (double.IsNaN(differenceSeconds))
                differenceSeconds = ((DateTime)connection.GetObject("SELECT CURRENT_TIMESTAMP") - DateTime.Now).TotalSeconds;

            return DateTime.Now.AddSeconds(differenceSeconds);
        }
    }
}
