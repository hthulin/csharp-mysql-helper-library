using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Uses a single connection. To be used with transactions and memory tables etc
    /// </summary>
    public class OneCon : Default, IDisposable
    {
        private IsolationLevel isolationLevel = IsolationLevel.Unspecified;
        private MySqlConnection mysqlConnection = null;
        private MySqlCommand mysqlCommand = null;
        private MySqlTransaction mysqlTransaction = null;
        private bool isTransaction = true;
        private bool disposed = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Connection string helper instance</param>
        /// <param name="isolationLevel">Specifies the transaction locking behaviour for the connection</param>
        /// <param name="isTransaction">Should be true when a transaction is to follow. If not, queries will be carried out as made</param>
        public OneCon(ConnectionString options, bool isTransaction = true, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            this.isolationLevel = isolationLevel;
            this.isTransaction = isTransaction;
            base.SetConnectionString(options);
            mysqlConnection = new MySqlConnection(base.connectionString);
            if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
            if (isTransaction) mysqlTransaction = mysqlConnection.BeginTransaction(this.isolationLevel);
            mysqlCommand = mysqlConnection.CreateCommand();
            mysqlCommand.Connection = mysqlConnection;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="isTransaction"></param>
        /// <param name="isolationLevel"></param>
        public OneCon(string connectionString, bool isTransaction = true, IsolationLevel isolationLevel = IsolationLevel.Serializable)
        {
            this.isolationLevel = isolationLevel;
            this.isTransaction = isTransaction;
            base.SetConnectionString(connectionString);
            mysqlConnection = new MySqlConnection(base.connectionString);
            if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
            if (isTransaction) mysqlTransaction = mysqlConnection.BeginTransaction(this.isolationLevel);
            mysqlCommand = mysqlConnection.CreateCommand();
            mysqlCommand.Connection = mysqlConnection;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                    try
                    {
                        if (mysqlTransaction != null) mysqlTransaction.Dispose();
                        if (mysqlCommand != null) mysqlCommand.Dispose();
                        if (mysqlConnection != null)
                            mysqlConnection.Dispose();
                    }
                    catch (Exception ex)
                    {
                        base.DiagnosticOutput("Dispose", ex.ToString());
                    }

                this.disposed = true;
            }
        }

        ~OneCon()
        {
            Dispose(false);
        }
        /// <summary>
        /// Commits transaction
        /// </summary>
        /// <param name="respring">if a new transaction is to follow, this should be true</param>
        public void Commit(bool respring = false)
        {
            base.DiagnosticOutput("Commit", "Commiting transaction");
            mysqlTransaction.Commit();

            if (respring) // Will make it possible to perform another transaction
            {
                Dispose(true);

                mysqlConnection = new MySqlConnection(base.connectionString);
                if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
                mysqlTransaction = mysqlConnection.BeginTransaction(this.isolationLevel);
                mysqlCommand = mysqlConnection.CreateCommand();
                mysqlCommand.Connection = mysqlConnection;
            }

            base.DiagnosticOutput("Commit", "Done");
        }

        /// <summary>
        /// Rolls the transaction back
        /// </summary>
        public void Rollback()
        {
            mysqlTransaction.Rollback();
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
            return base.InsertRow(this.mysqlCommand, database, table, onDuplicateUpdate, colData);
        }

        /// <summary>
        /// Inserts or updates a row and returns last insertion id. Generic data properties and data type must correspond to column names and column type
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="onDuplicateUpdate">If duplicate, update duplicate with new values</param>
        /// <param name="data">Instance where properties and type match database structure</param>
        /// <returns>Returns last insertion ID</returns>
        public override long InsertRowGeneric<T>(string database, string table, bool onDuplicateUpdate, T data)
        {
            return base.InsertRowGeneric<T>(this.mysqlCommand, database, table, onDuplicateUpdate, data);
        }

        /// <summary>
        /// Sets property field in instance based on the returned row from database
        /// </summary>
        public override void GetRowGeneric<T>(string query, T t, params ColumnData[] colData)
        {
            base.GetRowGeneric<T>(this.mysqlCommand, query, t, false, colData);
        }

        /// <summary>
        /// Sets property field in instance based on the returned row from database
        /// </summary>
        public override void GetRowGenericParse<T>(string query, T t, params ColumnData[] colData)
        {
            base.GetRowGeneric<T>(this.mysqlCommand, query, t, true, colData);
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
            return base.UpdateRow(this.mysqlCommand, database, table, where, limit, colData);
        }

        /// <summary>
        /// Sends query to server
        /// </summary>
        public override int SendQuery(string query, params ColumnData[] colData)
        {
            return base.SendQuery(this.mysqlCommand, query, colData);
        }

        /// <summary>
        /// Returns a field from the server as a object
        /// </summary>
        public override object GetObject(string query, params ColumnData[] colData)
        {
            return base.GetObject(this.mysqlCommand, query, colData);
        }

        /// Returns a field from the server as specified type using explicit type conversion
        /// Will throw exception if type is wrong
        public T GetObject<T>(string query, bool parse = false, params ColumnData[] colData)
        {
            if (parse)
                return Misc.Parsing.ParseObject<T>(GetObject(query, colData));
            else
                return (T)GetObject(query, colData);
        }

        /// Returns a field from the server as specified type by parsing value as string
        /// Will throw exception if type is wrong
        public T GetObjectParse<T>(string query, params ColumnData[] colData)
        {
            return GetObject<T>(query, true, colData);
        }

        /// <summary>
        /// Returns all selected data as a datatable
        /// </summary>
        public override DataTable GetDataTable(string query, params ColumnData[] colData)
        {
            return base.GetDataTable(mysqlCommand, query, colData);
        }

        /// <summary>
        /// Returns two dimensional object array with data
        /// </summary>
        /// <returns>Two dimensional object array</returns>
        public object[,] GetDataTableAsObjectArray2d(string query, params ColumnData[] colData)
        {
            using (DataTable dt = base.GetDataTable(mysqlCommand, query, colData))
            {
                object[,] returnData = new object[dt.Rows.Count, dt.Columns.Count];

                for (int row = 0; row < dt.Rows.Count; row++)
                    for (int col = 0; col < dt.Columns.Count; col++)
                        returnData[row, col] = dt.Rows[row][col];

                return returnData;
            }
        }

        /// <summary>
        /// Returns a ienumerable of instances.  Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="T">Instance type</typeparam>
        public override IEnumerable<T> GetIEnumerable<T>(string query, params ColumnData[] colData)
        {
            return base.GetIEnumerable<T>(mysqlCommand, query, false, colData);
        }

        /// <summary>
        /// Returns a ienumerable of instances. Instance property name and type must reflect table column name and type. Parses database content
        /// </summary>
        /// <typeparam name="T">Instance type</typeparam>
        public override IEnumerable<T> GetIEnumerableParse<T>(string query, params ColumnData[] colData)
        {
            return base.GetIEnumerable<T>(mysqlCommand, query, true, colData);
        }

        /// <summary>
        /// Returns a idictionary of instances. Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="Y">Key type</typeparam>
        /// <typeparam name="T">Instance type</typeparam>
        public override IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey, params ColumnData[] colData)
        {
            return base.GetIDictionary<Y, T>(mysqlCommand, keyColumn, query, parseKey, colData);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column number</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <param name="colData">Parameters</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, int column, bool parse, params ColumnData[] colData)
        {
            return base.GetColumn<T>(mysqlCommand, query, column, parse, colData);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column name</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <param name="colData">Parameters</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, string column, bool parse, params ColumnData[] colData)
        {
            return base.GetColumn<T>(mysqlCommand, query, column, parse);
        }

        /// <summary>
        /// Sends an entire collection to specified column
        /// </summary>
        public override long BulkSend(string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate)
        {
            return base.BulkSend(this.mysqlCommand, database, table, column, listData, onDuplicateUpdate);
        }

        /// <summary>
        /// Sends an entire datatable to specified table. Make sure that column names of table correspond to database
        /// </summary>
        public override long BulkSend(string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize = 100)
        {
            return base.BulkSend(this.mysqlCommand, database, table, dataTable, onDuplicateUpdate, updateBatchSize);
        }

        /// <summary>
        /// Sends generic IEnumerable to specified table and database. Make sure that the Generic properties and data type correspond
        /// to database column name and column type
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="listData"></param>
        /// <param name="onDuplicateUpdate"></param>
        public override long BulkSendGeneric<T>(string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate)
        {
            return base.BulkSendGeneric(this.mysqlCommand, database, table, listData, onDuplicateUpdate);
        }

    }
}