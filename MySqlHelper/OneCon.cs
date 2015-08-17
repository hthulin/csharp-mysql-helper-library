using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Uses a single connection. To be used with transactions and memory tables etc
    /// </summary>
    public class OneCon : XCon, IDisposable
    {
        #region Fields

        private bool disposed = false;
        private IsolationLevel isolationLevel = IsolationLevel.Unspecified;
        private bool isTransaction = true;
        private MySqlCommand mysqlCommand = null;
        private MySqlConnection mysqlConnection = null;
        private MySqlTransaction mysqlTransaction = null;

        #endregion Fields

        #region Constructors

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

        #endregion Constructors

        #region Destructors

        ~OneCon()
        {
            Dispose(false);
        }

        #endregion Destructors

        #region Methods

        /// <summary>
        /// Sends an entire collection to specified column
        /// </summary>
        public override long BulkSend(string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate, int updateBatchSize = 1000, bool continueUpdateOnError = false)
        {
            return base.BulkSend(this.mysqlCommand, database, table, column, listData, onDuplicateUpdate, updateBatchSize, continueUpdateOnError);
        }

        /// <summary>
        /// Sends an entire datatable to specified table. Make sure that column names of table correspond to database
        /// </summary>
        public override long BulkSend(string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize = 100, bool continueUpdateOnError = false)
        {
            return base.BulkSend(this.mysqlCommand, database, table, dataTable, onDuplicateUpdate, updateBatchSize, continueUpdateOnError);
        }

        /// <summary>
        /// Sends generic IEnumerable to specified table and database. Make sure that the Generic properties and data type correspond
        /// to database column name and column type
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="listData"></param>
        /// <param name="onDuplicateUpdate"></param>
        public override long BulkSendGeneric<T>(string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate, int updateBatchSize = 1000, bool continueUpdateOnError = false)
        {
            return base.BulkSendGeneric(this.mysqlCommand, database, table, listData, onDuplicateUpdate, updateBatchSize, continueUpdateOnError);
        }

        /// <summary>
        /// Commits transaction
        /// </summary>
        /// <param name="respring">if a new transaction is to follow, this should be true</param>
        public void Commit(bool respring = false)
        {
            mysqlTransaction.Commit();

            if (respring)
                ReinitializeConnection(); // Will make it possible to perform another transaction
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column number</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <param name="parameterData">Parameters</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, int column, bool parse, params ParameterData[] parameterData)
        {
            return base.GetColumn<T>(mysqlCommand, query, column, parse, parameterData);
        }

        /// <summary>
        /// Returns rows of selected column
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="query">Select query</param>
        /// <param name="column">Return column name</param>
        /// <param name="parse">Parses the object of explicit conversion</param>
        /// <param name="parameterData">Parameters</param>
        /// <returns>Selected data</returns>
        public override IEnumerable<T> GetColumn<T>(string query, string column, bool parse, params ParameterData[] parameterData)
        {
            return base.GetColumn<T>(mysqlCommand, query, column, parse, parameterData);
        }

        /// <summary>
        /// Returns all selected data as a datatable
        /// </summary>
        public override DataTable GetDataTable(string query, params ParameterData[] parameterData)
        {
            return base.GetDataTable(mysqlCommand, query, parameterData);
        }

        /// <summary>
        /// Returns two dimensional object array with data
        /// </summary>
        /// <returns>Two dimensional object array</returns>
        public object[,] GetDataTableAsObjectArray2d(string query, params ParameterData[] parameterData)
        {
            using (DataTable dt = base.GetDataTable(mysqlCommand, query, parameterData))
            {
                object[,] returnData = new object[dt.Rows.Count, dt.Columns.Count];

                for (int row = 0; row < dt.Rows.Count; row++)
                    for (int col = 0; col < dt.Columns.Count; col++)
                        returnData[row, col] = dt.Rows[row][col];

                return returnData;
            }
        }

        /// <summary>
        /// Returns a idictionary of instances. Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="Y">Key type</typeparam>
        /// <typeparam name="T">Instance type</typeparam>
        public override IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey, params ParameterData[] parameterData)
        {
            return base.GetIDictionary<Y, T>(mysqlCommand, keyColumn, query, parseKey, parameterData);
        }

        /// <summary>
        /// Returns a ienumerable of instances.  Instance property name and type must reflect table column name and type
        /// </summary>
        /// <typeparam name="T">Instance type</typeparam>
        public override IEnumerable<T> GetIEnumerable<T>(string query, params ParameterData[] parameterData)
        {
            return base.GetIEnumerable<T>(mysqlCommand, query, false, parameterData);
        }

        /// <summary>
        /// Returns a ienumerable of instances. Instance property name and type must reflect table column name and type. Parses database content
        /// </summary>
        /// <typeparam name="T">Instance type</typeparam>
        public override IEnumerable<T> GetIEnumerableParse<T>(string query, params ParameterData[] parameterData)
        {
            return base.GetIEnumerable<T>(mysqlCommand, query, true, parameterData);
        }

        /// <summary>
        /// Returns a field from the server as a object
        /// </summary>
        public override object GetObject(string query, params ParameterData[] parameterData)
        {
            return base.GetObject(this.mysqlCommand, query, parameterData);
        }

        /// Returns a field from the server as specified type using explicit type conversion
        /// Will throw exception if type is wrong
        public T GetObject<T>(string query, bool parse = false, params ParameterData[] parameterData)
        {
            if (parse)
                return Misc.Parsing.ParseObject<T>(GetObject(query, parameterData));
            else
                return (T)GetObject(query, parameterData);
        }

        /// Returns a field from the server as specified type by parsing value as string
        /// Will throw exception if type is wrong
        public T GetObjectParse<T>(string query, params ParameterData[] parameterData)
        {
            return GetObject<T>(query, true, parameterData);
        }

        /// <summary>
        /// Sets property field in instance based on the returned row from database
        /// </summary>
        public override void GetRowGeneric<T>(string query, T t, params ParameterData[] parameterData)
        {
            base.GetRowGeneric<T>(this.mysqlCommand, query, t, false, parameterData);
        }

        /// <summary>
        /// Sets property field in instance based on the returned row from database
        /// </summary>
        public override void GetRowGenericParse<T>(string query, T t, params ParameterData[] parameterData)
        {
            base.GetRowGeneric<T>(this.mysqlCommand, query, t, true, parameterData);
        }

        /// <summary>
        /// Inserts a row and returns last insertion id
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="parameterData">Columns and their data</param>
        /// <param name="onDuplicateUpdate">If duplicate, update duplicate with new values</param>
        /// <returns>Returns last insertion ID</returns>
        public override long InsertRow(string database, string table, bool onDuplicateUpdate, params ParameterData[] parameterData)
        {
            return base.InsertRow(this.mysqlCommand, database, table, onDuplicateUpdate, parameterData);
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

        public override void ReadRowIntoGeneric(string database, string table, string keyColumn, object keyData, object inst)
        {
            base.ReadRowIntoGeneric(this.mysqlCommand, database, table, keyColumn, keyData, inst);
        }

        /// <summary>
        /// Rolls the transaction back
        /// </summary>
        public void Rollback(bool respring = false)
        {
            mysqlTransaction.Rollback();

            if (respring)
                ReinitializeConnection(); // Will make it possible to perform another transaction
        }

        /// <summary>
        /// Sends query to server
        /// </summary>
        public override int SendQuery(string query, params ParameterData[] parameterData)
        {
            return base.SendQuery(this.mysqlCommand, query, parameterData);
        }

        /// <summary>
        /// Updates a row or rows
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="where">Which row(s) to update, null = all</param>
        /// <param name="limit">amount of rows to update. 0 = all</param>
        /// <param name="parameterData">Columns and their data</param>
        /// <returns>Returns update count</returns>
        public override long UpdateRow(string database, string table, string where, int limit, params ParameterData[] parameterData)
        {
            return base.UpdateRow(this.mysqlCommand, database, table, where, limit, parameterData);
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
                        base.DebugOutput("Dispose", ex.ToString());
                    }

                this.disposed = true;
            }
        }

        private void ReinitializeConnection()
        {
            Dispose(true);

            mysqlConnection = new MySqlConnection(base.connectionString);
            if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
            mysqlTransaction = mysqlConnection.BeginTransaction(this.isolationLevel);
            mysqlCommand = mysqlConnection.CreateCommand();
            mysqlCommand.Connection = mysqlConnection;
        }

        #endregion Methods
    }
}