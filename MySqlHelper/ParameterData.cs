using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Container class for column name and column data. Also used for parameter data
    /// </summary>
    public class ParameterData
    {
        public string parameterName { get; set; }

        private object _parameterData = null;
        public object parameterData
        {
            get
            {
                if (_parameterData != null && _parameterData.GetType() == typeof(double) && double.IsNaN((double)_parameterData))
                    return null;

                return _parameterData;
            }
            set
            {
                _parameterData = value;
            }
        }

        public ParameterData() { }

        public ParameterData(string parameterName, object parameterData)
        {
            this.parameterName = parameterName;
            this.parameterData = parameterData;
        }

        public override string ToString()
        {
            return string.Format("{0} : {1}", parameterName, parameterData);
        }

        public string GetParameterWhereString()
        {
            if (parameterData == null)
                return string.Format("`{0}` IS NULL", parameterName);
            else
                return string.Format("`{0}`=@{0}", parameterName);
        }

        public MySqlParameter GetMysqlParameter()
        {
            return new MySqlParameter("@" + parameterName, parameterData);
        }

        public static IEnumerable<ParameterData> GetGenericWriteMysqlParameters<T>(T data) where T : new()
        {
            return typeof(T).GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties())).Select(n => new ParameterData(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, typeof(T).GetProperties()), n.GetValue(data, null)));
        }
    }

    /// <summary>
    /// Legacy support
    /// </summary>
    public class ColumnData : ParameterData
    {
        public string columnName
        {
            get { return base.parameterName; }
            set { base.parameterName = value; }
        }

        public object data
        {
            get { return base.parameterData; }
            set { base.parameterData = value; }
        }

        public ColumnData(string columnName, object cellValue)
        {
            base.parameterName = columnName;
            base.parameterData = cellValue;
        }
    }
}
