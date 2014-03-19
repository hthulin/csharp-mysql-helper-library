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

        public Misc.Log logData = new Misc.Log();
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
            DiagnosticOutput("InsertRow", "Database " + database + " table " + table + " data " + string.Join(", ", listColData.ToList()));

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", listColData.Select(n => n.columnName)) + "`) VALUES (" + string.Join(",", listColData.Select(n => "@" + n.columnName)) + ")";

            if (onDuplicateUpdate)
            {
                mysqlCommand.CommandText += " ON DUPLICATE KEY UPDATE `" + string.Join(", `", listColData.Select(n => n.columnName + "`=@" + n.columnName));
            }

            mysqlCommand.Parameters.AddRange(listColData.Select(n => n.GetMysqlParameter()).ToArray());

            logData.IncreaseUpdates((ulong)mysqlCommand.ExecuteNonQuery());

            mysqlCommand.Parameters.Clear();

            return mysqlCommand.LastInsertedId;
        }

        public abstract long InsertRowGeneric<T>(string database, string table, bool onDuplicateUpdate, T data) where T : new();
        internal long InsertRowGeneric<T>(MySqlCommand mysqlCommand, string database, string table, bool onDuplicateUpdate, T data) where T : new()
        {
            return InsertRow(mysqlCommand, database, table, onDuplicateUpdate, typeof(T).GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties())).Select(n => new ColumnData(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, typeof(T).GetProperties()), n.GetValue(data, null))).ToArray());
        }

        public abstract long UpdateRow(string database, string table, string where, int limit, params ColumnData[] colData);
        internal long UpdateRow(MySqlCommand mysqlCommand, string database, string table, string where, int limit, params ColumnData[] colData)
        {
            DiagnosticOutput("UpdateRow", "Database " + database + " table " + table + "data " + string.Join(", ", colData.ToList()));

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = "UPDATE `" + database + "`.`" + table + "` SET `" + string.Join(", `", colData.Select(n => n.columnName + "`=@" + n.columnName)) + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where) + (limit == 0 ? "" : " LIMIT " + limit.ToString() + ";");

            mysqlCommand.Parameters.AddRange(colData.Select(n => n.GetMysqlParameter()).ToArray());

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

        public T GetRow<T>(DataRow row, PropertyInfo[] properties, bool parse) where T : new()
        {
            T returnData = new T();

            Misc.MysqlTableAttributeFunctions.LoadDataRowIntoGeneric<T>(row, returnData, parse);

            return returnData;
        }

        public abstract IEnumerable<T> GetIEnumerable<T>(string query, params ColumnData[] colData) where T : new();
        public abstract IEnumerable<T> GetIEnumerableParse<T>(string query, params ColumnData[] colData) where T : new();
        internal IEnumerable<T> GetIEnumerable<T>(MySqlCommand mysqlCommand, string query, bool parse, params ColumnData[] colData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(row => GetRow<T>(row, properties, parse));
        }

        public abstract IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey, params ColumnData[] colData) where T : new();
        internal IDictionary<Y, T> GetIDictionary<Y, T>(MySqlCommand mysqlCommand, string keyColumn, string query, bool parseKey, params ColumnData[] colData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, colData).AsEnumerable().ToDictionary(row => parseKey ? Misc.Parsing.ParseObject<Y>(row[keyColumn]) : (Y)row[keyColumn], row => GetRow<T>(row, properties, false));
        }

        public abstract void GetRowGenericParse<T>(string query, T t, params ColumnData[] colData) where T : new();
        public abstract void GetRowGeneric<T>(string query, T t, params ColumnData[] colData) where T : new();
        internal void GetRowGeneric<T>(MySqlCommand mysqlCommand, string query, T t, bool parse, params ColumnData[] colData) where T : new()
        {
            using (DataTable dataTable = GetDataTable(mysqlCommand, query, colData))
                Misc.MysqlTableAttributeFunctions.LoadDataRowIntoGeneric<T>(dataTable.Rows[0], t, parse);
        }
    
        public abstract long BulkSend(string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize = 100);
        internal long BulkSend(MySqlCommand mysqlCommand, string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize = 100)
        {
            try
            {
                IEnumerable<string> columnNames = dataTable.Columns.Cast<DataColumn>().Select(n => n.ColumnName);

                DiagnosticOutput("BulkSend", string.Format("Database {0} Table {1} Columns {2}", database, table, string.Join(", ", columnNames)));

                logData.IncreaseQueries(1);

                mysqlCommand.Parameters.AddRange(columnNames.Select(n => new MySqlParameter() { ParameterName = "@" + n, SourceColumn = n }).ToArray());

                mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", columnNames) + "`) VALUES (" + string.Join(",", columnNames.Select(n => "@" + n)) + ") ";

                if (onDuplicateUpdate) mysqlCommand.CommandText += "ON DUPLICATE KEY UPDATE `" + string.Join(", `", columnNames.Select(n => n + "`=@" + n));

                mysqlCommand.CommandType = CommandType.Text;
                mysqlCommand.UpdatedRowSource = UpdateRowSource.None;

                using (MySqlDataAdapter adapter = new MySqlDataAdapter())
                {
                    adapter.ContinueUpdateOnError = true;
                    adapter.InsertCommand = mysqlCommand;
                    adapter.UpdateBatchSize = updateBatchSize;
                    long l = adapter.Update(dataTable);
                    logData.IncreaseUpdates((ulong)l);
                    return l;
                }
            }
            finally
            {
                mysqlCommand.Parameters.Clear();
            }
        }

        public abstract long BulkSend(string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate);
        internal long BulkSend(MySqlCommand mysqlCommand, string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate)
        {
            using (DataTable dataTable = new DataTable())
            {
                dataTable.Columns.Add(column);
                listData.All(n => { dataTable.Rows.Add(n); return true; });
                return BulkSend(mysqlCommand, database, table, dataTable, onDuplicateUpdate);
            }
        }

        public abstract long BulkSendGeneric<T>(string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate);
        internal long BulkSendGeneric<T>(MySqlCommand mysqlCommand, string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate)
        {
            using (DataTable dataTable = new DataTable())
            {
                dataTable.Columns.AddRange(typeof(T).GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties())).Select(n =>
                    new DataColumn(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, typeof(T).GetProperties()), typeof(object))
                    ).ToArray());

                foreach (T data in listData)
                {
                    dataTable.Rows.Add(data.GetType().GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties()))
                        .Select(n =>
                            n.GetValue(data, null) == null ? null : n.GetValue(data, null).GetType() == typeof(double) ? double.IsNaN((double)n.GetValue(data, null)) ? null : n.GetValue(data, null) : n.GetValue(data, null))
                        .ToArray());
                }

                return BulkSend(mysqlCommand, database, table, dataTable, onDuplicateUpdate, 1000);
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
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => Misc.Parsing.ParseObject<T>(n[(int)column]));
                else
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => Misc.Parsing.ParseObject<T>(n[column.ToString()]));
            }
            else
            {
                if (column.GetType() == typeof(int))
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => n[(int)column]).Cast<T>();
                else
                    return GetDataTable(mysqlCommand, query, colData).AsEnumerable().Select(n => n[column.ToString()]).Cast<T>();
            }
        }

    }
}