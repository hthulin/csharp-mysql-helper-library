using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Uses only one connection. To be used with transactions and memory tables etc
    /// </summary>
    public class OneCon : Default, IDisposable
    {
        private MySqlConnection mysqlConnection = null;
        private MySqlCommand mysqlCommand = null;
        private MySqlTransaction mysqlTransaction = null;
        private bool isTransaction = true;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        /// <param name="isTransaction">Should be true when a transaction is to follow. If not, queries will be carried out as made</param>
        public OneCon(ConnectionString options, bool isTransaction = true)
        {
            this.isTransaction = isTransaction;
            base.SetConnectionString(options);
            mysqlConnection = new MySqlConnection(base.connectionString);
            if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
            if (isTransaction) mysqlTransaction = mysqlConnection.BeginTransaction();
            mysqlCommand = mysqlConnection.CreateCommand();
            mysqlCommand.Connection = mysqlConnection;
        }

        /// <summary>
        /// Disposes all data. Should always run after use of OneCon
        /// </summary>
        public void Dispose()
        {
            if (mysqlTransaction != null) mysqlTransaction.Dispose();
            if (mysqlCommand != null) mysqlCommand.Dispose();
            if (mysqlConnection != null)
            {
                mysqlConnection.Close();
                mysqlConnection.Dispose();
            }
        }

        /// <summary>
        /// Commits transaction
        /// </summary>
        /// <param name="respring">if a new transaction is to follow, this should be true</param>
        public void Commit(bool respring = false)
        {
            mysqlTransaction.Commit();

            if (respring) // Will make it possible to perform another transaction
            {
                Dispose();
                mysqlConnection = new MySqlConnection(base.connectionString);
                if (!base.OpenConnection(mysqlConnection, 10)) throw new Exception("Unable to connect");
                mysqlTransaction = mysqlConnection.BeginTransaction();
                mysqlCommand = mysqlConnection.CreateCommand();
                mysqlCommand.Connection = mysqlConnection;
            }
        }

        /// <summary>
        /// Rolls the transaction back
        /// </summary>
        public void Rollback()
        {
            mysqlTransaction.Rollback();
        }

        /// <summary>
        /// Inserts a row
        /// </summary>
        /// <param name="database">Destination database</param>
        /// <param name="table">Destination table</param>
        /// <param name="listColData">Columns and their data</param>
        /// <param name="updateWhere">Where query if to update existing row</param>
        /// <param name="onDupeUpdate">If duplicate, update duplicate with new values</param>
        /// <returns>Returns last insertion ID</returns>
        public override long InsertRow(string database, string table, IEnumerable<ColumnData> listColData, string updateWhere = null, bool onDupeUpdate = false)
        {
            return base.InsertRow(this.mysqlCommand, database, table, listColData, updateWhere, onDupeUpdate);
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

        /// <summary>
        /// Returns a field from the server as specified type 
        /// </summary>
        public T GetObject<T>(string query)
        {
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
        /// Sends an entire datatable to specified table
        /// </summary>
        public override void BulkSend(string database, string table, DataTable dataTable)
        {
            base.BulkSend(this.mysqlCommand, database, table, dataTable);
        }

        /// <summary>
        /// Returns a list containing the first field of each row
        /// </summary>
        public override IEnumerable<T> GetFirst<T>(string query)
        {
            return base.GetFirst<T>(this.mysqlCommand, query);
        }
    }
}