using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySql.MysqlHelper
{
    /// <summary>
    /// Container class for column name and column data
    /// </summary>
    public class ColumnData
    {
        public string columnName { get; set; }

        private object _data = null;
        public object data
        {
            get
            {
                if (_data != null && _data.GetType() == typeof(double) && double.IsNaN((double)_data))
                    return null;

                return _data;
            }
            set
            {
                _data = value;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ColumnData(string columnName, object cellValue)
        {
            this.columnName = columnName;
            this.data = cellValue;
        }

        public override string ToString()
        {
            return string.Format("{0} : {1}", columnName, data);
        }

        public string GetParameterWhereString()
        {
            if (data == null)
                return string.Format("`{0}` IS NULL", columnName);
            else
                return string.Format("`{0}`=@{0}", columnName);
        }

        public MySqlParameter GetMysqlParameter()
        {
            return new MySqlParameter("@" + columnName, data);
        }
    }
}
