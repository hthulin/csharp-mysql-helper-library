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
        public string columnName;
        public object data;

        /// <summary>
        /// Constructor
        /// </summary>
        public ColumnData(string columnName, object cellValue)
        {
            this.columnName = columnName;
            this.data = cellValue;
        }
    }

    /// <summary>
    /// Helper for generating connection string
    /// </summary>
    public class ConnectionString
    {
        public string server;
        public string username;
        public string password;
        public uint port = 3306;
        public bool pooling = true;
        public bool compress = false;
        public uint defaultCommandTimeout = 5000;
        public uint connectionTimeout = 5000;
        public bool convertZeroDateTime = true;
        public bool allowUserVariables = true;

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionString(string server, string uid, string pwd, uint port = 3306)
        {
            this.server = server;
            this.username = uid;
            this.password = pwd;
            this.port = port;
        }

        /// <summary>
        /// ToString() override
        /// </summary>
        /// <returns>Returns a valid connection string</returns>
        public override string ToString()
        {
            return
            "Server=" + server +
            ";Port=" + port.ToString() +
            ";Uid=" + username.ToString() +
            ";Pwd=" + password.ToString() +
            ";AllowUserVariables=" + allowUserVariables.ToString() +
            ";ConnectionTimeout=" + connectionTimeout.ToString() +
            ";DefaultCommandTimeout=" + defaultCommandTimeout.ToString() +
            ";ConvertZeroDateTime=" + convertZeroDateTime.ToString() +
            ";Pooling=" + pooling.ToString() +
            ";Compress=" + compress.ToString();
        }
    }
}