using System;
using System.Collections.Generic;
using System.Linq;

namespace MySql.MysqlHelper.Misc
{
    public class ChecksumCheck
    {
        #region Fields

        private Dictionary<Tuple<string, string>, long> dictCheckSumData = new Dictionary<Tuple<string, string>, long>();
        private DateTime lastCheck = new DateTime(1970, 1, 1);

        #endregion Fields

        #region Methods

        public bool HasChanged(MultiCon multiCon, string database, string table, uint checkIntervalSeconds = 0)
        {
            if (checkIntervalSeconds > 0)
            {
                if ((DateTime.Now - lastCheck).TotalSeconds < checkIntervalSeconds)
                    return false;
                else
                    lastCheck = DateTime.Now;
            }

            Tuple<string, string> tuple = new Tuple<string, string>(database, table);

            long checksum = GetChecksum(multiCon, database, table);

            if (!dictCheckSumData.ContainsKey(tuple))
            {
                dictCheckSumData.Add(tuple, checksum);
                return true;
            }

            bool changed = dictCheckSumData[tuple] != checksum;

            if (changed)
            {
                dictCheckSumData[tuple] = checksum;
                return true;
            }

            return false;
        }

        private long GetChecksum(MultiCon multiCon, string database, string table)
        {
            return multiCon.GetColumn<long>("CHECKSUM TABLE  `" + database + "`.`" + table + "` EXTENDED", 1, false).First();
        }

        #endregion Methods
    }
}