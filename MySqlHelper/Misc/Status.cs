using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySql.MysqlHelper.Misc
{
    public class Status
    {
        private double differenceSeconds = double.NaN;

        private XCon xCon = null;

        /// <summary>
        /// Returns server timestamp. Only needs to query the database once.
        /// </summary>
        /// <returns></returns>
        public DateTime GetServerDateTime()
        {
            if (double.IsNaN(differenceSeconds))
                differenceSeconds = ((DateTime)xCon.GetObject("SELECT CURRENT_TIMESTAMP") - DateTime.Now).TotalSeconds;

            return DateTime.Now.AddSeconds(differenceSeconds);
        }

        public Status(XCon xCon)
        {
            this.xCon = xCon;
        }
    }
}
