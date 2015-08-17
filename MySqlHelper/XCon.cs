using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace MySql.MysqlHelper
{
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public abstract class XCon
    {
        #region Fields

        public readonly uint id = GetID();

        public Misc.Log logData = new Misc.Log();

        private static readonly object _lock = new object();

        private static uint ids = 0;

        private ConnectionString connectionStringOptions = null;

        #endregion Fields

        #region Properties

        /// <summary>
        /// Current connectionstring in use
        /// </summary>
        public string connectionString { get; set; }

        #endregion Properties

        #region Methods

        public abstract long BulkSend(string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize = 1000, bool continueUpdateOnError = false);

        public abstract long BulkSend(string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate, int updateBatchSize = 1000, bool continueUpdateOnError = false);

        public abstract long BulkSendGeneric<T>(string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate, int updateBatchSize = 1000, bool continueUpdateOnError = false);

        public void DebugOutput(string method, string text)
        {
            System.Diagnostics.Debug.WriteLine(text, string.Format("MySQL ({0}) {1}", this.id, method));
        }

        public abstract IEnumerable<T> GetColumn<T>(string query, string column, bool parse, params ParameterData[] parameterData);

        public abstract IEnumerable<T> GetColumn<T>(string query, int column, bool parse, params ParameterData[] parameterData);

        public ConnectionString GetConnectionOptions()
        {
            return this.connectionStringOptions;
        }

        public string GetConnectionString()
        {
            return this.connectionString;
        }

        public abstract DataTable GetDataTable(string query, params ParameterData[] parameterData);

        public abstract IDictionary<Y, T> GetIDictionary<Y, T>(string keyColumn, string query, bool parseKey, params ParameterData[] parameterData) where T : new();

        public abstract IEnumerable<T> GetIEnumerable<T>(string query, params ParameterData[] parameterData) where T : new();

        public abstract IEnumerable<T> GetIEnumerableParse<T>(string query, params ParameterData[] parameterData) where T : new();

        public abstract object GetObject(string query, params ParameterData[] parameterData);

        public T GetRow<T>(DataRow row, PropertyInfo[] properties, bool parse) where T : new()
        {
            T returnData = new T();

            Misc.MysqlTableAttributeFunctions.LoadDataRowIntoGeneric<T>(row, returnData, parse);

            return returnData;
        }

        public abstract void GetRowGeneric<T>(string query, T t, params ParameterData[] parameterData) where T : new();

        public abstract void GetRowGenericParse<T>(string query, T t, params ParameterData[] parameterData) where T : new();

        public abstract long InsertRow(string database, string table, bool onDuplicateUpdate, params ParameterData[] parameterData);

        public abstract long InsertRowGeneric<T>(string database, string table, bool onDuplicateUpdate, T data) where T : new();

        /// <summary>
        /// Opens a connection to the server
        /// </summary>
        public bool OpenConnection(MySqlConnection mysqlConnection, int attempts)
        {
            for (int i = 0; i < (connectionStringOptions != null ? connectionStringOptions.connectionAttemps : attempts); i++)
            {
                try
                {
                    mysqlConnection.Open();
                    break;
                }
                catch (MySqlException)
                {
                    if (i == attempts - 1) throw;
                    DebugOutput("OpenConnection", "Exception. Trying to connect again");
                    System.Threading.Thread.Sleep(connectionStringOptions != null ? connectionStringOptions.connectionSleep : 50);
                }
            }
            return mysqlConnection.State == ConnectionState.Open;
        }

        public abstract void ReadRowIntoGeneric(string database, string table, string keyColumn, object keyData, object inst);

        public abstract int SendQuery(string query, params ParameterData[] parameterData);

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

        public abstract long UpdateRow(string database, string table, string where, int limit, params ParameterData[] parameterData);

        internal long BulkSend(MySqlCommand mysqlCommand, string database, string table, DataTable dataTable, bool onDuplicateUpdate, int updateBatchSize, bool continueUpdateOnError)
        {
            try
            {
                IEnumerable<string> columnNames = dataTable.Columns.Cast<DataColumn>().Select(n => n.ColumnName);

                DebugOutput("BulkSend", string.Format("Database {0} Table {1} Columns {2}", database, table, string.Join(", ", columnNames)));

                logData.IncreaseQueries(1);

                mysqlCommand.Parameters.AddRange(columnNames.Select(n => new MySqlParameter() { ParameterName = "@" + n, SourceColumn = n }).ToArray());

                mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", columnNames) + "`) VALUES (" + string.Join(",", columnNames.Select(n => "@" + n)) + ") ";

                if (onDuplicateUpdate) mysqlCommand.CommandText += "ON DUPLICATE KEY UPDATE `" + string.Join(", `", columnNames.Select(n => n + "`=@" + n));

                mysqlCommand.CommandType = CommandType.Text;
                mysqlCommand.UpdatedRowSource = UpdateRowSource.None;

                using (MySqlDataAdapter adapter = new MySqlDataAdapter())
                {
                    adapter.ContinueUpdateOnError = continueUpdateOnError;
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

        internal long BulkSend(MySqlCommand mysqlCommand, string database, string table, string column, IEnumerable<object> listData, bool onDuplicateUpdate, int updateBatchSize, bool continueUpdateOnError)
        {
            using (DataTable dataTable = new DataTable())
            {
                dataTable.Columns.Add(column);
                listData.All(n => { dataTable.Rows.Add(n); return true; });
                return BulkSend(mysqlCommand, database, table, dataTable, onDuplicateUpdate, updateBatchSize, continueUpdateOnError);
            }
        }

        internal long BulkSendGeneric<T>(MySqlCommand mysqlCommand, string database, string table, IEnumerable<T> listData, bool onDuplicateUpdate, int updateBatchSize, bool continueUpdateOnError)
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

                return BulkSend(mysqlCommand, database, table, dataTable, onDuplicateUpdate, updateBatchSize, continueUpdateOnError);
            }
        }

        internal IEnumerable<T> GetColumn<T>(MySqlCommand mysqlCommand, string query, object column, bool parse, params ParameterData[] parameterData)
        {
            DebugOutput("GetColumn <" + typeof(T).ToString() + "> (" + parameterData.ToString() + ")" + (parse ? " PARSE" : ""), query);

            logData.IncreaseQueries(1);

            if (parse)
            {
                if (column.GetType() == typeof(int))
                    return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().Select(n => Misc.Parsing.ParseObject<T>(n[(int)column]));
                else
                    return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().Select(n => Misc.Parsing.ParseObject<T>(n[column.ToString()]));
            }
            else
            {
                if (column.GetType() == typeof(int))
                    return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().Select(n => n[(int)column]).Cast<T>();
                else
                    return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().Select(n => n[column.ToString()]).Cast<T>();
            }
        }

        internal DataTable GetDataTable(MySqlCommand mysqlCommand, string query, params ParameterData[] parameterData)
        {
            DebugOutput("GetDataTable", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;

            if (parameterData != null && parameterData.Count() > 0 && parameterData[0] != null) mysqlCommand.Parameters.AddRange(parameterData.Select(n => n.GetMysqlParameter()).ToArray());

            DataTable dataTable = new DataTable();

            using (MySqlDataAdapter mysqlDataAdapter = new MySqlDataAdapter(mysqlCommand))
                mysqlDataAdapter.Fill(dataTable);

            if (parameterData != null) mysqlCommand.Parameters.Clear();

            return dataTable;
        }

        internal IDictionary<Y, T> GetIDictionary<Y, T>(MySqlCommand mysqlCommand, string keyColumn, string query, bool parseKey, params ParameterData[] parameterData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().ToDictionary(row => parseKey ? Misc.Parsing.ParseObject<Y>(row[keyColumn]) : (Y)row[keyColumn], row => GetRow<T>(row, properties, false));
        }

        internal IEnumerable<T> GetIEnumerable<T>(MySqlCommand mysqlCommand, string query, bool parse, params ParameterData[] parameterData) where T : new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            return GetDataTable(mysqlCommand, query, parameterData).AsEnumerable().Select(row => GetRow<T>(row, properties, parse));
        }

        internal object GetObject(MySqlCommand mysqlCommand, string query, params ParameterData[] parameterData)
        {
            try
            {
                DebugOutput("GetObject", query);

                logData.IncreaseQueries(1);

                mysqlCommand.CommandText = query;

                if (parameterData != null) mysqlCommand.Parameters.AddRange(parameterData.Select(n => n.GetMysqlParameter()).ToArray());

                return mysqlCommand.ExecuteScalar();
            }
            finally
            {
                if (parameterData != null) mysqlCommand.Parameters.Clear();
            }
        }

        internal void GetRowGeneric<T>(MySqlCommand mysqlCommand, string query, T t, bool parse, params ParameterData[] parameterData) where T : new()
        {
            using (DataTable dataTable = GetDataTable(mysqlCommand, query, parameterData))
                Misc.MysqlTableAttributeFunctions.LoadDataRowIntoGeneric<T>(dataTable.Rows[0], t, parse);
        }

        internal long InsertRow(MySqlCommand mysqlCommand, string database, string table, bool onDuplicateUpdate, params ParameterData[] parameterData)
        {
            DebugOutput("InsertRow", "Database " + database + " table " + table + " data " + string.Join(", ", parameterData.ToList()));

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = "INSERT INTO `" + database + "`.`" + table + "` (`" + string.Join("`,`", parameterData.Select(n => n.parameterName)) + "`) VALUES (" + string.Join(",", parameterData.Select(n => "@" + n.parameterName)) + ")";

            if (onDuplicateUpdate)
            {
                mysqlCommand.CommandText += " ON DUPLICATE KEY UPDATE `" + string.Join(", `", parameterData.Select(n => n.parameterName + "`=@" + n.parameterName));
            }

            mysqlCommand.Parameters.AddRange(parameterData.Select(n => n.GetMysqlParameter()).ToArray());

            logData.IncreaseUpdates((ulong)mysqlCommand.ExecuteNonQuery());

            mysqlCommand.Parameters.Clear();

            return mysqlCommand.LastInsertedId;
        }

        internal long InsertRowGeneric<T>(MySqlCommand mysqlCommand, string database, string table, bool onDuplicateUpdate, T data) where T : new()
        {
            return InsertRow(mysqlCommand, database, table, onDuplicateUpdate, typeof(T).GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties())).Select(n => new ParameterData(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, typeof(T).GetProperties()), n.GetValue(data, null))).ToArray());
        }

        internal void ReadRowIntoGeneric(MySqlCommand mysqlCommand, string database, string table, string keyColumn, object keyData, object inst)
        {
            using (DataTable dataTable = GetDataTable(mysqlCommand, "SELECT `" + string.Join("`,`", Misc.MysqlTableAttributeFunctions.GetReadColumnNames(inst.GetType())) + "` FROM `" + database + "`.`" + table + "` WHERE `" + keyColumn + "`=@keyData LIMIT 1", new ParameterData("keyData", keyData)))
            {
                Misc.MysqlTableAttributeFunctions.LoadDataRowIntoGeneric(dataTable.Rows[0], inst, false);
            }
        }

        internal int SendQuery(MySqlCommand mysqlCommand, string query, params ParameterData[] parameterData)
        {
            DebugOutput("SendQuery", query);

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = query;

            if (parameterData != null) mysqlCommand.Parameters.AddRange(parameterData.Select(n => n.GetMysqlParameter()).ToArray());

            int countUpdates = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)countUpdates);

            if (parameterData != null) mysqlCommand.Parameters.Clear();

            return countUpdates;
        }

        internal long UpdateRow(MySqlCommand mysqlCommand, string database, string table, string where, int limit, params ParameterData[] parameterData)
        {
            DebugOutput("UpdateRow", "Database " + database + " table " + table + "data " + string.Join(", ", parameterData.ToList()));

            logData.IncreaseQueries(1);

            mysqlCommand.CommandText = "UPDATE `" + database + "`.`" + table + "` SET `" + string.Join(", `", parameterData.Select(n => n.parameterName + "`=@" + n.parameterName)) + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where) + (limit == 0 ? "" : " LIMIT " + limit.ToString() + ";");

            mysqlCommand.Parameters.AddRange(parameterData.Select(n => n.GetMysqlParameter()).ToArray());

            long updateCount = mysqlCommand.ExecuteNonQuery();

            logData.IncreaseUpdates((ulong)updateCount);

            mysqlCommand.Parameters.Clear();

            return updateCount;
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

        #endregion Methods
    }
}