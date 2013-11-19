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
        private static readonly object _lock = new object();
        private static uint ids = 0;
        public readonly uint id = GetID();

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
        /// Generates instance ID
        /// </summary>
        /// <returns>Current Instance ID</returns>
        private static uint GetID()
        {
            lock (_lock)
                return ids++;
        }

        public void DiagnosticOutput(string method, string text)
        {
            System.Diagnostics.Debug.WriteLine(string.Format("({0}) MySQL {1} : {2}", this.id, method, text));
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
                    System.Diagnostics.Debug.WriteLine("Exception. Trying to connect again", "(" + id + ") MySQL");
                    System.Threading.Thread.Sleep(50);
                }
            }
            return mysqlConnection.State == ConnectionState.Open;
        }

        public abstract long InsertRow(string database, string table, IEnumerable<ColumnData> listColData, bool onDupeUpdate = false);
        internal long InsertRow(MySqlCommand mysqlCommand, string database, string table, IEnumerable<ColumnData> listColData, bool onDupeUpdate = false)
        {
            DiagnosticOutput("InsertRow", "Database " + database + " table " + table + "data " + string.Join(", ", listColData));

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

        public abstract long UpdateRow(string database, string table, IEnumerable<ColumnData> listColData, string where = null, int limit = 0);
        internal long UpdateRow(MySqlCommand mysqlCommand, string database, string table, IEnumerable<ColumnData> listColData, string where = null, int limit = 0)
        {
            DiagnosticOutput("UpdateRow", "Database " + database + " table " + table + "data " + string.Join(", ", listColData));

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

        public abstract int SendQuery(string query);
        internal int SendQuery(MySqlCommand mysqlCommand, string query)
        {
            DiagnosticOutput("SendQuery", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;
            int countUpdates = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)countUpdates);

            return countUpdates;
        }

        public abstract object GetObject(string query);
        internal object GetObject(MySqlCommand mysqlCommand, string query)
        {
            DiagnosticOutput("GetObject", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;
            return mysqlCommand.ExecuteScalar();
        }

        public abstract DataTable GetDataTable(string query);
        internal DataTable GetDataTable(MySqlConnection mysqlConnection, string query)
        {
            DiagnosticOutput("GetDataTable", query);

            logData.IncreaseQueries(1);

            using (DataSet ds = new DataSet())
            {
                using (MySqlDataAdapter _adapter = new MySqlDataAdapter(query, mysqlConnection))
                    _adapter.Fill(ds, "map");

                return ds.Tables[0];
            }
        }

        public abstract void BulkSend(string database, string table, DataTable dataTable, int updateBatchSize = 100);
        internal void BulkSend(MySqlCommand mysqlCommand, string database, string table, DataTable dataTable, int updateBatchSize = 100)
        {
            DiagnosticOutput("BulkSend", "Database " + database + " table " + table);

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

            mysqlCommand.Parameters.Clear();
        }

        public abstract void BulkSend(string database, string table, string column, IEnumerable<object> listData);
        internal void BulkSend(MySqlCommand mysqlCommand, string database, string table, string column, IEnumerable<object> listData)
        {
            using (DataTable dataTable = new DataTable())
            {
                dataTable.Columns.Add(column);
                listData.All(n => { dataTable.Rows.Add(n); return true; });
                BulkSend(mysqlCommand, database, table, dataTable);
            }
        }

        public abstract IEnumerable<T> GetFirst<T>(string query, bool parse = false);
        internal IEnumerable<T> GetFirst<T>(MySqlCommand mysqlCommand, string query, bool parse = false)
        {
            DiagnosticOutput("GetFirst <" + typeof(T).ToString() + ">" + (parse ? " PARSE" : ""), query);
            System.Diagnostics.Debug.WriteLine(query, "(" + id + ") MySQL GetFirst (" + typeof(T) + ") " + (parse ? "PARSE" : ""));

            logData.IncreaseQueries(1);

            List<T> returnData = new List<T>();
            mysqlCommand.CommandText = query;
            using (MySqlDataReader _datareader = mysqlCommand.ExecuteReader())
            {
                if (parse)
                    while (_datareader.Read()) returnData.Add(ParseObject<T>(_datareader[0]));
                else
                    while (_datareader.Read()) returnData.Add((T)_datareader[0]);
            }
            return returnData;
        }

        internal T ParseObject<T>(object o)
        {
            Type newType = typeof(T);

            if (newType == typeof(int))
                return (T)Convert.ChangeType(int.Parse(o.ToString()), newType);

            if (newType == typeof(uint))
                return (T)Convert.ChangeType(uint.Parse(o.ToString()), newType);

            if (newType == typeof(long))
                return (T)Convert.ChangeType(long.Parse(o.ToString()), newType);

            if (newType == typeof(ulong))
                return (T)Convert.ChangeType(ulong.Parse(o.ToString()), newType);

            if (newType == typeof(short))
                return (T)Convert.ChangeType(short.Parse(o.ToString()), newType);

            if (newType == typeof(ushort))
                return (T)Convert.ChangeType(ushort.Parse(o.ToString()), newType);

            if (newType == typeof(double))
                return (T)Convert.ChangeType(double.Parse(o.ToString().Replace(',', '.')), newType, System.Globalization.CultureInfo.InvariantCulture);

            if (newType == typeof(float))
                return (T)Convert.ChangeType(float.Parse(o.ToString().Replace(',', '.')), newType, System.Globalization.CultureInfo.InvariantCulture);

            if (newType == typeof(byte))
                return (T)Convert.ChangeType(byte.Parse(o.ToString()), newType);

            if (newType == typeof(string))
                return (T)Convert.ChangeType(o.ToString(), newType);

            if (newType == typeof(bool))
                return (T)Convert.ChangeType(bool.Parse(o.ToString()), newType);

            if (newType.IsEnum)
                return (T)Convert.ChangeType(Enum.Parse(newType, o.ToString()), newType);

            throw new Exception("No such type defined for parsing");
        }

    }
}