using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    public abstract class Default
    {
        /// <summary>
        /// Current connectionstring in use
        /// </summary>
        public string connectionString { get; set; }
        private ConnectionString connectionStringOptions = null;
        public Log logData = new Log();

        /// <summary>
        /// Sets connection string
        /// </summary>
        public void SetConnectionString(ConnectionString options)
        {
            this.connectionStringOptions = options;
            this.connectionString = options.ToString();
        }

        public ConnectionString GetConnectionOptions()
        {
            return this.connectionStringOptions;
        }

        public string GetConnectionString()
        {
            return this.connectionString;
        }

        /// <summary>
        /// Opens a connection to the server
        /// </summary>
        public bool OpenConnection(MySqlConnection mysqlConnection, int attempts)
        {
            for (int i = 0; i < attempts; i++)
            {
                try
                {
                    mysqlConnection.Open();
                    break;
                }
                catch (MySqlException)
                {
                    if (i == attempts - 1) throw;
                    System.Threading.Thread.Sleep(50);
                }
            }
            return mysqlConnection.State == ConnectionState.Open;
        }

        internal long InsertRow(MySqlCommand mysqlCommand, string database, string table, IEnumerable<ColumnData> listColData, bool onDupeUpdate = false)
        {
            logData.IncreaseQueries(1);

            int i = 0;

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", listColData.Select(n => n.columnName)) + "`) VALUES (" + string.Join(",", listColData.Select(n => "@" + (i++))) + ")";

            i = 0;

            mysqlCommand.Parameters.AddRange(listColData.Select(n => new MySqlParameter("@" + (i++), n.data)).ToArray());

            if (onDupeUpdate)
            {
                i = 0;

                mysqlCommand.CommandText += " ON DUPLICATE KEY UPDATE `" + string.Join(", `", listColData.Select(n => n.columnName + "`=@" + (i++)));
            }

            logData.IncreaseUpdates((ulong)mysqlCommand.ExecuteNonQuery());

            mysqlCommand.Parameters.Clear();

            return mysqlCommand.LastInsertedId;
        }

        public abstract long InsertRow(string database, string table, IEnumerable<ColumnData> listColData, bool onDupeUpdate = false);

        internal long UpdateRow(MySqlCommand mysqlCommand, string database, string table, IEnumerable<ColumnData> listColData, string where = null, int limit = 0)
        {
            logData.IncreaseQueries(1);

            int i = 0;

            mysqlCommand.CommandText = "UPDATE `" + database + "`.`" + table + "` SET `" + string.Join(", `", listColData.Select(n => n.columnName + "`=@" + (i++))) + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where) + (limit == 0 ? "" : " LIMIT " + limit.ToString() + ";");

            i = 0;

            mysqlCommand.Parameters.AddRange(listColData.Select(n => new MySqlParameter("@" + (i++), n.data)).ToArray());

            long updateCount = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)updateCount);

            mysqlCommand.Parameters.Clear();

            return updateCount;
        }

        public abstract long UpdateRow(string database, string table, IEnumerable<ColumnData> listColData, string where = null, int limit = 0);

        internal int SendQuery(MySqlCommand mysqlCommand, string query)
        {
            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;
            int countUpdates = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)countUpdates);

            return countUpdates;
        }

        public abstract int SendQuery(string query);

        internal object GetObject(MySqlCommand mysqlCommand, string query)
        {
            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;
            return mysqlCommand.ExecuteScalar();
        }

        public abstract object GetObject(string query);

        internal DataTable GetDataTable(MySqlConnection mysqlConnection, string query)
        {
            logData.IncreaseQueries(1);

            using (DataSet ds = new DataSet())
            {
                using (MySqlDataAdapter _adapter = new MySqlDataAdapter(query, mysqlConnection))
                    _adapter.Fill(ds, "map");

                return ds.Tables[0];
            }
        }

        public abstract DataTable GetDataTable(string query);

        internal void BulkSend(MySqlCommand mysqlCommand, string database, string table, DataTable dataTable, int updateBatchSize = 100)
        {
            logData.IncreaseQueries(1);

            Dictionary<string, string> dictIds = new Dictionary<string, string>();

            int i = 0;

            foreach (DataColumn column in dataTable.Columns)
            {
                dictIds.Add(column.ColumnName, "?s" + (i++).ToString());
                mysqlCommand.Parameters.Add(dictIds[column.ColumnName], MySqlDbType.String).SourceColumn = column.ColumnName;
            }

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (" + string.Join(",", dictIds.Select(n => n.Key)) + ") VALUES (" + string.Join(",", dictIds.Select(n => n.Value)) + ");";
            mysqlCommand.CommandType = CommandType.Text;
            mysqlCommand.UpdatedRowSource = UpdateRowSource.None;

            using (MySqlDataAdapter adapter = new MySqlDataAdapter())
            {
                adapter.ContinueUpdateOnError = true;
                adapter.InsertCommand = mysqlCommand;
                adapter.UpdateBatchSize = updateBatchSize;
                logData.IncreaseUpdates((ulong)adapter.Update(dataTable));
            }
        }

        public abstract void BulkSend(string database, string table, DataTable dataTable, int updateBatchSize = 100);

        internal void BulkSend(MySqlCommand mysqlCommand, string database, string table, string column, IEnumerable<object> listData)
        {
            using (DataTable dataTable = new DataTable())
            {
                dataTable.Columns.Add(column);
                listData.All(n => { dataTable.Rows.Add(n); return true; });
                BulkSend(mysqlCommand, database, table, dataTable);
            }
        }

        public abstract void BulkSend(string database, string table, string column, IEnumerable<object> listData);

        internal IEnumerable<T> GetFirst<T>(MySqlCommand mysqlCommand, string query)
        {
            logData.IncreaseQueries(1);

            List<T> returnData = new List<T>();
            mysqlCommand.CommandText = query;
            using (MySqlDataReader _datareader = mysqlCommand.ExecuteReader())
            {
                while (_datareader.Read())
                    returnData.Add((T)_datareader[0]);
            }

            return returnData;
        }

        public abstract IEnumerable<T> GetFirst<T>(string query);

    }
}