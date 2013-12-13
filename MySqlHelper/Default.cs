using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Reflection;
using MySql.Data.MySqlClient;

namespace MySql.MysqlHelper
{
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
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
        /// Sets connection string through helper instance
        /// </summary>
        public void SetConnectionString(ConnectionString options)
        {
            this.connectionStringOptions = options;
            this.connectionString = options.ToString();
        }

        public void SetConnectionString(string connectionString)
        {
            this.connectionString = connectionString;
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

        public abstract long InsertRow(string database, string table, bool onDuplicateUpdate, params ColumnData[] listColData);
        internal long InsertRow(MySqlCommand mysqlCommand, string database, string table, bool onDuplicateUpdate, params ColumnData[] listColData)
        {
            DiagnosticOutput("InsertRow", "Database " + database + " table " + table + "data " + string.Join(", ", listColData.ToList()));

            logData.IncreaseQueries(1);

            int i = 0;

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", listColData.Select(n => n.columnName)) + "`) VALUES (" + string.Join(",", listColData.Select(n => "@" + (i++))) + ")";

            i = 0;

            mysqlCommand.Parameters.AddRange(listColData.Select(n => new MySqlParameter("@" + (i++), n.data)).ToArray());

            if (onDuplicateUpdate)
            {
                i = 0;
                mysqlCommand.CommandText += " ON DUPLICATE KEY UPDATE `" + string.Join(", `", listColData.Select(n => n.columnName + "`=@" + (i++)));
            }

            logData.IncreaseUpdates((ulong)mysqlCommand.ExecuteNonQuery());

            mysqlCommand.Parameters.Clear();

            return mysqlCommand.LastInsertedId;
        }

        public abstract long InsertRowGeneric<T>(string database, string table, bool onDuplicateUpdate, T data) where T : new();
        internal long InsertRowGeneric<T>(MySqlCommand mysqlCommand, string database, string table, bool onDuplicateUpdate, T data) where T : new()
        {
            return InsertRow(mysqlCommand, database, table, onDuplicateUpdate, typeof(T).GetProperties().Select(n => new ColumnData(n.Name, n.GetValue(data, null))).ToArray());
        }

        public abstract long UpdateRow(string database, string table, string where, int limit, params ColumnData[] colData);
        internal long UpdateRow(MySqlCommand mysqlCommand, string database, string table, string where, int limit, params ColumnData[] colData)
        {
            DiagnosticOutput("UpdateRow", "Database " + database + " table " + table + "data " + string.Join(", ", colData.ToList()));

            logData.IncreaseQueries(1);

            int i = 0;

            mysqlCommand.CommandText = "UPDATE `" + database + "`.`" + table + "` SET `" + string.Join(", `", colData.Select(n => n.columnName + "`=@" + (i++))) + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where) + (limit == 0 ? "" : " LIMIT " + limit.ToString() + ";");

            i = 0;

            mysqlCommand.Parameters.AddRange(colData.Select(n => new MySqlParameter("@" + (i++), n.data)).ToArray());

            long updateCount = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)updateCount);

            mysqlCommand.Parameters.Clear();

            return updateCount;
        }

        public abstract int SendQuery(string query, params ColumnData[] colData);
        internal int SendQuery(MySqlCommand mysqlCommand, string query, params ColumnData[] colData)
        {
            DiagnosticOutput("SendQuery", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;

            if (colData != null) mysqlCommand.Parameters.AddRange(colData.Select(n => n.GetMysqlParameter()).ToArray());

            int countUpdates = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)countUpdates);

            if (colData != null) mysqlCommand.Parameters.Clear();

            return countUpdates;
        }

        public abstract object GetObject(string query, params ColumnData[] colData);
        internal object GetObject(MySqlCommand mysqlCommand, string query, params ColumnData[] colData)
        {
            try
            {
                DiagnosticOutput("GetObject", query);

                logData.IncreaseQueries(1);

                mysqlCommand.CommandText = query;

                if (colData != null) mysqlCommand.Parameters.AddRange(colData.Select(n => n.GetMysqlParameter()).ToArray());

                return mysqlCommand.ExecuteScalar();
            }
            finally
            {
                if (colData != null) mysqlCommand.Parameters.Clear();
            }
        }

        public abstract DataTable GetDataTable(string query, params ColumnData[] colData);
        internal DataTable GetDataTable(MySqlCommand mysqlCommand, string query, params ColumnData[] colData)
        {
            DiagnosticOutput("GetDataTable", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;

            if (colData != null) mysqlCommand.Parameters.AddRange(colData.Select(n => n.GetMysqlParameter()).ToArray());

            DataTable dataTable = new DataTable();

            using (MySqlDataAdapter mysqlDataAdapter = new MySqlDataAdapter(mysqlCommand))
                mysqlDataAdapter.Fill(dataTable);

            if (colData != null) mysqlCommand.Parameters.Clear();

            return dataTable;
        }

        private T GetRow<T>(DataRow row, PropertyInfo[] properties) where T : new()
        {
            T returnData = new T();

            foreach (PropertyInfo property in properties)
            {
                if (!DBNull.Value.Equals(row[property.Name]))
                    property.SetValue(returnData, row[property.Name], null);
            }

            return returnData;
        }

        public abstract IEnumerable<T> GetIEnumerable<T>(string query, params ColumnData[] colData) where T : new();
        internal IEnumerable<T> GetIEnumerable<T>(MySqlCommand mysqlCommand, string query, params ColumnData[] colData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(row => GetRow<T>(row, properties));
        }

        public abstract IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey, params ColumnData[] colData) where T : new();
        internal IDictionary<Y, T> GetIDictionary<Y, T>(MySqlCommand mysqlCommand, string keyColumn, string query, bool parseKey, params ColumnData[] colData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, colData).AsEnumerable().ToDictionary(row => parseKey ? ParseObject<Y>(row[keyColumn]) : (Y)row[keyColumn], row => GetRow<T>(row, properties));
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

        public abstract IEnumerable<T> GetColumn<T>(string query, string column, bool parse, params ColumnData[] colData);
        public abstract IEnumerable<T> GetColumn<T>(string query, int column, bool parse, params ColumnData[] colData);
        internal IEnumerable<T> GetColumn<T>(MySqlCommand mysqlCommand, string query, object column, bool parse, params ColumnData[] colData)
        {
            DiagnosticOutput("GetColumn <" + typeof(T).ToString() + "> (" + column.ToString() + ")" + (parse ? " PARSE" : ""), query);

            logData.IncreaseQueries(1);

            if (parse)
            {
                if (column.GetType() == typeof(int))
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => ParseObject<T>(n[(int)column]));
                else
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => ParseObject<T>(n[column.ToString()]));
            }
            else
            {
                if (column.GetType() == typeof(int))
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => n[(int)column]).Cast<T>();
                else
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => n[column.ToString()]).Cast<T>();
            }
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