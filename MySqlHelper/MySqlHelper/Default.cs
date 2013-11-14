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
        private ConnectionString connectionStringOptions;
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
                catch (MySqlException ex)
                {
                    System.Diagnostics.Debug.WriteLine("OpenConnect failed attempt " + (i + 1) + "\n" + ex);
                    if (i == attempts - 1) throw ex;
                    System.Threading.Thread.Sleep(50);
                }
            }
            return mysqlConnection.State == ConnectionState.Open;
        }

        internal long InsertRow(MySqlCommand mysqlCommand, string database, string table, IEnumerable<ColumnData> listColData, string updateWhere = null, bool onDupeUpdate = false)
        {
            logData.IncreaseQueries(1);

            string valuetags = "";

            for (int i = 0; i < listColData.Count(); i++)
            {
                if (i != 0) valuetags += ",";
                valuetags += "@p" + i.ToString();
            }

            if (updateWhere == null)
            {
                mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", listColData.Select(n => n.columnName)) + "`) VALUES (" + valuetags + ")";


                for (int i = 0; i < listColData.Count(); i++)
                    mysqlCommand.Parameters.AddWithValue("@p" + i.ToString(), listColData.ElementAt(i).data);

                if (onDupeUpdate)
                {
                    mysqlCommand.CommandText += " ON DUPLICATE KEY UPDATE ";
                    for (int col = 0; col < listColData.Count(); col++)
                    {
                        if (col != 0) mysqlCommand.CommandText += ",";
                        mysqlCommand.CommandText += "`" + listColData.ElementAt(col).columnName + "`=@p" + col.ToString();
                    }
                }
            }
            else
            {
                mysqlCommand.CommandText = string.Empty;

                for (int i = 0; i < listColData.Count(); i++)
                {
                    mysqlCommand.CommandText += "UPDATE `" + database + "`.`" + table + "` SET `" + listColData.ElementAt(i).columnName + "`=@p" + i.ToString() + "x" + " WHERE " + updateWhere + " LIMIT 1;";
                    mysqlCommand.Parameters.AddWithValue("@p" + i.ToString() + "x", listColData.ElementAt(i).data);
                }
            }

            logData.IncreaseUpdates((ulong)mysqlCommand.ExecuteNonQuery());
            mysqlCommand.Parameters.Clear();

            return mysqlCommand.LastInsertedId;
        }

        public abstract long InsertRow(string database, string table, IEnumerable<ColumnData> listColData, string updateWhere = null, bool onDupeUpdate = false);

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

        internal void BulkSend(MySqlCommand mysqlCommand, string database, string table, DataTable dataTable)
        {
            logData.IncreaseQueries(1);

            List<string> columnNames = new List<string>();
            List<string> columnIds = new List<string>();

            foreach (DataColumn column in dataTable.Columns)
            {
                columnNames.Add(column.ColumnName);
                columnIds.Add("?s" + columnNames.Count().ToString());
            }

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (" + string.Join(",", columnNames) + ") VALUES (" + string.Join(",", columnIds) + ");";

            mysqlCommand.CommandType = CommandType.Text;
            mysqlCommand.UpdatedRowSource = UpdateRowSource.None;

            for (int i = 0; i < columnNames.Count; i++)
            {
                mysqlCommand.Parameters.Add(columnIds[i], MySqlDbType.String).SourceColumn = columnNames[i];
            }

            using (MySqlDataAdapter adapter = new MySqlDataAdapter())
            {
                adapter.ContinueUpdateOnError = true;
                adapter.InsertCommand = mysqlCommand;
                adapter.UpdateBatchSize = 100;
                logData.IncreaseUpdates((ulong)adapter.Update(dataTable));
            }
        }

        public abstract void BulkSend(string database, string table, DataTable dataTable);

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