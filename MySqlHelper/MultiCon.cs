using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Opens and closes a connection for each query. When not doing transactions or working with memory tables
    /// </summary>
    public class MultiCon : Default
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Connection string helper instance</param>
        public MultiCon(ConnectionString options)
        {
            base.SetConnectionString(options);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        public MultiCon(string connectionString)
        {
            base.SetConnectionString(connectionString);
        }

        /// <summary>
        /// Inserts a row and returns last insertion id
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="colData">Columns and their data</param>
        /// <param name="onDuplicateUpdate">If duplicate, update duplicate with new values</param>
        /// <returns>Returns last insertion ID</returns>
        public override long InsertRow(string database, string table, bool onDuplicateUpdate, params ColumnData[] colData)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                return base.InsertRow(mysqlCommand, database, table, onDuplicateUpdate, colData);
        }

        /// <summary>
        /// Updates a row or rows
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="where">Which row(s) to update, null = all</param>
        /// <param name="limit">amount of rows to update. 0 = all</param>
        /// <param name="colData">Columns and their data</param>
        /// <returns>Returns update count</returns>
        public override long UpdateRow(string database, string table, string where, int limit, params ColumnData[] colData)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                return base.UpdateRow(mysqlCommand, database, table, where, limit, colData);
        }

        /// <summary>
        /// Sends an entire collection to specified column
        /// </summary>
        public override void BulkSend(string database, string table, string column, IEnumerable<object> listData)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                base.BulkSend(mysqlCommand, database, table, column, listData);
        }

        /// <summary>
        /// Sends an entire datatable to specified table. Make sure that column names of table correspond to database
        /// </summary>
        public override void BulkSend(string database, string table, DataTable dataTable, int updateBatchSize = 100)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                base.BulkSend(mysqlCommand, database, table, dataTable, updateBatchSize);
        }

 
        /// <summary>
        /// Returns a field from the server as a object
        /// </summary>
        public override object GetObject(string query)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                return base.GetObject(mysqlCommand, query);
        }

        /// <summary>
        /// Returns a field from the server as specified type using explicit type conversion.
        /// Will throw exception if type is wrong
        /// </summary>
        public T GetObject<T>(string query, bool parse = false)
        {
            if (parse)
                return base.ParseObject<T>(GetObject(query));
            else
                return (T)GetObject(query);
        }

        /// <summary>
        /// Sends query to server
        /// </summary>
        public override int SendQuery(string query)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
            using (MySqlCommand mysqlCommand = mysqlConnection.CreateCommand())
                return base.SendQuery(mysqlCommand, query);
        }

        /// <summary>
        /// Returns all selected data as a datatable
        /// </summary>
        public override DataTable GetDataTable(string query)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
                return base.GetDataTable(mysqlConnection, query);
        }

        /// <summary>
        /// Returns a ienumerable of instances.  Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="T">Instance type</typeparam>
        public override IEnumerable<T> GetIEnumerable<T>(string query)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
                return base.GetIEnumerable<T>(mysqlConnection, query);
        }

        /// <summary>
        /// Returns a idictionary of instances. Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="Y">Key type</typeparam>
        /// <typeparam name="T">Instance type</typeparam>
        public override IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey = false)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
                return base.GetIDictionary<Y, T>(mysqlConnection, keyColumn, query, parseKey);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column number</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, int column, bool parse = false)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
                return base.GetColumn<T>(mysqlConnection, query, column, parse);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column name</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, string column, bool parse = false)
        {
            using (MySqlConnection mysqlConnection = GetMysqlConnection())
                return base.GetColumn<T>(mysqlConnection, query, column, parse);
        }

        /// <summary>
        /// Returns the default connecition data
        /// </summary>
        private MySqlConnection GetMysqlConnection()
        {
            MySqlConnection mysqlConnection = new MySqlConnection(base.connectionString);
            if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
            return mysqlConnection;
        }
    }
}