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
        /// <param name="options"></param>
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
        /// Updates a row or rows
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="colData">Columns and their data</param>
        /// <param name="where">Which row(s) to update, null = all</param>
        /// <param name="limit">amount of rows to update. 0 = all</param>
        /// <returns>Returns update count</returns>
        public override long UpdateRow(string database, string table, string where, int limit, params ColumnData[] colData)
        {
            return base.UpdateRow(this.mysqlCommand, database, table, where, limit, colData);
        }

        /// <summary>
        /// Sends query to server
        /// </summary>
        public override int SendQuery(string query)
        {
            return base.SendQuery(this.mysqlCommand, query);
        }

        /// <summary>
        /// Returns a field from the server as a object
        /// </summary>
        public override object GetObject(string query)
        {
            return base.GetObject(this.mysqlCommand, query);
        }

        /// Returns a field from the server as specified type using explicit type conversion.
        /// Will throw exception if type is wrong
        public T GetObject<T>(string query, bool parse = false)
        {
            if (parse)
                return base.ParseObject<T>(GetObject(query));
            else
                return (T)GetObject(query);
        }

        /// <summary>
        /// Returns all selected data as a datatable
        /// </summary>
        public override DataTable GetDataTable(string query)
        {
            return base.GetDataTable(mysqlConnection, query);
        }

        /// <summary>
        /// Sends an entire collection to specified column
        /// </summary>
        public override void BulkSend(string database, string table, string column, IEnumerable<object> listData)
        {
            base.BulkSend(this.mysqlCommand, database, table, column, listData);
        }

        /// <summary>
        /// Sends an entire datatable to specified table. Make sure that column names of table correspond to database
        /// </summary>
        public override void BulkSend(string database, string table, DataTable dataTable, int updateBatchSize = 100)
        {
            base.BulkSend(this.mysqlCommand, database, table, dataTable, updateBatchSize);
        }

        /// <summary>
        /// Returns a list containing the first field of each row
        /// <param name="parse">Parses the object as a string instead of explicit conversion</param>
        /// </summary>
        public override IEnumerable<T> GetFirst<T>(string query, bool parse = false)
        {
            return base.GetFirst<T>(this.mysqlCommand, query, parse);
        }


    }
}