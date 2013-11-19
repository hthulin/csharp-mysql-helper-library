using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Log class
    /// </summary>
    public class Log
    {
        private readonly object _lock = new object();

        private ulong mysqlQueries = 0;
        private ulong mysqlUpdates = 0;

        public ulong GetAndResetMysqlUpdates()
        {
            try
            {
                lock (_lock)
                    return mysqlUpdates;
            }
            finally
            {
                lock (_lock)
                    mysqlUpdates = 0;
            }
        }

        public ulong GetAndResetMysqlQueries()
        {
            try
            {
                lock (_lock)
                    return mysqlQueries;
            }
            finally
            {
                lock (_lock)
                    mysqlQueries = 0;
            }
        }

        public ulong GetMysqlQueries()
        {
            lock (_lock) return mysqlQueries;
        }

        public ulong GetMysqlUpdates()
        {
            lock (_lock) return mysqlUpdates;
        }

        public void ResetUpdatesAndQueriesCount()
        {
            lock (_lock)
            {
                mysqlQueries = 0;
                mysqlUpdates = 0;
            }
        }

        public void IncreaseUpdates(ulong updates)
        {
            lock (_lock)
                mysqlUpdates += updates;
        }

        public void IncreaseQueries(ulong queries)
        {
            lock (_lock)
                mysqlQueries += queries;
        }
    }

    /// <summary>
    /// Container class for column name and column data
    /// </summary>
    public class ColumnData
    {
        public string columnName { get; set; }
        public object data { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ColumnData(string columnName, object cellValue)
        {
            this.columnName = columnName;
            this.data = cellValue;
        }

        public override string ToString()
        {
            return string.Format("`{0}` = '{1}'", columnName, data);
        }
    }
}