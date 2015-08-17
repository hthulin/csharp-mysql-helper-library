using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Linq;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Legacy support
    /// </summary>
    public class ColumnData : ParameterData
    {
        #region Constructors

        public ColumnData(string columnName, object cellValue)
        {
            base.parameterName = columnName;
            base.parameterData = cellValue;
        }

        #endregion Constructors

        #region Properties

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

        #endregion Properties
    }

    /// <summary>
    /// Container class for column name and column data. Also used for parameter data
    /// </summary>
    public class ParameterData
    {
        #region Fields

        private object _parameterData = null;

        #endregion Fields

        #region Constructors

        public ParameterData()
        {
        }

        public ParameterData(string parameterName, object parameterData)
        {
            this.parameterName = parameterName;
            this.parameterData = parameterData;
        }

        #endregion Constructors

        #region Properties

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

        public string parameterName { get; set; }

        #endregion Properties

        #region Methods

        public static IEnumerable<ParameterData> GetGenericWriteMysqlParameters<T>(T data) where T : new()
        {
            return typeof(T).GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, typeof(T).GetProperties())).Select(n => new ParameterData(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, typeof(T).GetProperties()), n.GetValue(data, null)));
        }

        public static IEnumerable<ParameterData> GetGenericWriteMysqlParameters(object data)
        {
            return data.GetType().GetProperties().Where(n => Misc.MysqlTableAttributeFunctions.GetPropertyShouldWrite(n, data.GetType().GetProperties())).Select(n => new ParameterData(Misc.MysqlTableAttributeFunctions.GetPropertyDatabaseColumnName(n, data.GetType().GetProperties()), n.GetValue(data, null)));
        }

        public MySqlParameter GetMysqlParameter()
        {
            return new MySqlParameter("@" + parameterName, parameterData);
        }

        public string GetParameterWhereString()
        {
            if (parameterData == null)
                return string.Format("`{0}` IS NULL", parameterName);
            else
                return string.Format("`{0}`=@{0}", parameterName);
        }

        public override string ToString()
        {
            return string.Format("{0} : {1}", parameterName, parameterData);
        }

        #endregion Methods
    }
}