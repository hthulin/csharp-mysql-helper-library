using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace MySql.MysqlHelper.InformationSchema
{
    public class TableUpdateTimeCacheAll
    {
        #region Fields

        private readonly object _lock = new object();

        private string checkOnlyDatabase = null;
        private Dictionary<Tuple<string, string>, DateTime> informationSchemaCache = null;
        private DateTime lastUpdateOfCache = new DateTime(1970, 1, 1);
        private MultiCon multiCon = null;
        private Dictionary<Tuple<string, string, string>, DateTime> questionCache = new Dictionary<Tuple<string, string, string>, DateTime>();

        #endregion Fields

        #region Constructors

        /// <summary>
        /// Constructor for connection string
        /// </summary>
        public TableUpdateTimeCacheAll(ConnectionString connectionString, string onlyCheckDatabase = null)
        {
            this.checkOnlyDatabase = onlyCheckDatabase;
            this.multiCon = new MultiCon(connectionString);
        }

        /// <summary>
        /// Constructor for connection instance
        /// </summary>
        public TableUpdateTimeCacheAll(MultiCon multiCon, string onlyCheckDatabase = null)
        {
            this.checkOnlyDatabase = onlyCheckDatabase;
            this.multiCon = multiCon;
        }

        #endregion Constructors

        #region Methods

        /// <summary>
        /// Determains if a MyISAM table has been updated. Always returns true on first call.
        /// Keeps an cache of the entire update time information for all the tables.
        /// Answer is given in the context of the askerID, so this instance may be shared
        /// between multiple tasks.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <param name="updateCacheIntervalTimeSpan"></param>
        /// <param name="askerId"></param>
        /// <param name="returnFalseIfTableDoesNotExist"></param>
        /// <returns></returns>
        public bool HasChanged(string database, string table, TimeSpan updateCacheIntervalTimeSpan, string askerId = null, bool returnFalseIfTableDoesNotExist = true)
        {
            lock (_lock)
            {
                if (informationSchemaCache == null || (DateTime.Now - lastUpdateOfCache) >= updateCacheIntervalTimeSpan)
                {
                    informationSchemaCache = multiCon.GetDataTable(
                        string.Format("SELECT `TABLE_SCHEMA`,`TABLE_NAME`,`UPDATE_TIME` FROM `information_schema`.`TABLES` WHERE {0} `UPDATE_TIME` IS NOT NULL",
                            string.IsNullOrWhiteSpace(checkOnlyDatabase) ? string.Empty : "`TABLE_SCHEMA`=@databasename &&"),
                            string.IsNullOrWhiteSpace(checkOnlyDatabase) ? null : new ParameterData("databasename", checkOnlyDatabase))
                            .AsEnumerable()
                            .ToDictionary(n =>
                                new Tuple<string, string>(n.Field<string>("TABLE_SCHEMA"), n.Field<string>("TABLE_NAME")), n => n.Field<DateTime>("UPDATE_TIME")
                                );

                    lastUpdateOfCache = DateTime.Now;
                }

                if (!informationSchemaCache.ContainsKey(new Tuple<string, string>(database, table)))
                    if (returnFalseIfTableDoesNotExist)
                        return false;
                    else
                        throw new Exception("Table does not exist in Information Schema!");

                Tuple<string, string, string> identifier = new Tuple<string, string, string>(askerId, database, table);

                DateTime lastModified = informationSchemaCache[new Tuple<string, string>(database, table)];

                if (!questionCache.ContainsKey(identifier))
                    questionCache.Add(identifier, lastModified);
                else
                    if (questionCache[identifier] == lastModified)
                    return false;
                else
                    questionCache[identifier] = lastModified;
            }
            return true;
        }

        /// <summary>
        /// Sets current connection instance
        /// </summary>
        /// <param name="multiCon"></param>
        public void SetMultiCon(MultiCon multiCon)
        {
            this.multiCon = multiCon;
        }

        #endregion Methods
    }
}