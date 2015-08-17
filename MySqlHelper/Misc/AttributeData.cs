using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MySql.MysqlHelper.Misc
{
    /// <summary>
    /// Attributes. WriteData & ReadData is default True.
    /// DatabaseColumnName is default null. Then column name will equal property name
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property, AllowMultiple = false)]
    public class MySqlColumnAttribute : Attribute
    {
        #region Fields

        public string DatabaseColumnName = null;
        public string MysqlCreateType = null;
        public bool ReadFromDatabase = true;
        public bool WriteToDatabase = true;

        #endregion Fields
    }

    public class MysqlTableAttributeFunctions
    {
        #region Methods

        /// <summary>
        /// Alters table based on attribute "MysqlCreateType" information
        /// </summary>
        /// <param name="t">Type of object</param>
        /// <param name="database">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="xcon">Multicon / Onecon instance</param>
        /// <param name="dropUnusedColumns">Removes unused columns</param>
        /// <returns></returns>
        public static string GetAlterTableString(Type t, string database, string table, XCon xcon, bool dropUnusedColumns)
        {
            StringBuilder sb = new StringBuilder();

            List<string> existingDatabaseColumns = xcon.GetColumn<string>("SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` LIKE @database AND `TABLE_NAME` LIKE @table", 0, false, new ParameterData("database", database), new ParameterData("table", table)).ToList();

            Dictionary<string, string> existingTypeColumns = new Dictionary<string, string>();

            foreach (var prop in t.GetProperties())
            {
                foreach (var cust in prop
                    .GetCustomAttributes(false)
                    .Where(n =>
                        n != null && n.GetType() == typeof(MySqlColumnAttribute))
                        .Select(n => (MySqlColumnAttribute)n)
                    )
                {
                    existingTypeColumns.Add(string.IsNullOrWhiteSpace(cust.DatabaseColumnName) ? prop.Name : cust.DatabaseColumnName, cust.MysqlCreateType);
                }
            }

            if (dropUnusedColumns)
                foreach (string remove in existingDatabaseColumns
                    .Where(n => !existingTypeColumns.ContainsKey(n)))
                {
                    sb.Append("ALTER TABLE `" + database + "`.`" + table + "` DROP COLUMN " + remove + ";\n");
                }

            foreach (var add in existingTypeColumns
                .Where(n => !existingDatabaseColumns.Any(n2 => n2.IndexOf(n.Key, StringComparison.InvariantCultureIgnoreCase) > -1)))
            {
                sb.Append("ALTER TABLE `" + database + "`.`" + table + "` ADD " + add.Key + " " + add.Value + ";\n");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Creates new table based on attribute "MysqlCreateType" information
        /// </summary>
        /// <param name="t">Object type</param>
        /// <param name="database">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="createIfNotExists">Creates only if not exist</param>
        /// <param name="primaryKey">Primary key</param>
        /// <param name="engine">Database engine</param>
        /// <returns></returns>
        public static string GetCreateTableString(Type t, string database, string table, bool createIfNotExists = false, string primaryKey = null, string engine = "MyISAM")
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("CREATE TABLE " + (createIfNotExists ? "IF NOT EXISTS" : "") + "`" + database + "`.`" + table + "` (");

            List<string> cols = new List<string>();

            foreach (var prop in t.GetProperties().OrderBy(n => string.IsNullOrWhiteSpace(primaryKey) ? 0 : (n.Name.ToLower() == primaryKey.ToLower() ? 0 : 1)))
            {
                foreach (var cust in prop
                    .GetCustomAttributes(false)
                    .Where(n =>
                        n != null && n.GetType() == typeof(MySqlColumnAttribute))
                        .Select(n => (MySqlColumnAttribute)n)
                    )
                {
                    cols.Add((string.IsNullOrWhiteSpace(cust.DatabaseColumnName) ? prop.Name : cust.DatabaseColumnName) + " " + cust.MysqlCreateType);
                }
            }

            if (!string.IsNullOrWhiteSpace(primaryKey))
                cols.Add("PRIMARY KEY (`" + primaryKey + "`)");

            sb.Append(string.Join(", ", cols));

            sb.Append(") ENGINE=" + engine);

            return sb.ToString();
        }

        /// <summary>
        /// Returns wheter or not the property uses the mysqltable attribute
        /// </summary>
        public static bool GetHasProperty(System.Reflection.PropertyInfo property)
        {
            return property.GetCustomAttributes(false) != null && property.GetCustomAttributes(false).Any(n2 => n2.GetType() == typeof(MySqlColumnAttribute));
        }

        public static IEnumerable<System.Reflection.PropertyInfo> GetPropertiesWriteColumn(Type t)
        {
            return t.GetProperties().Where(n => GetPropertyShouldWrite(n, t.GetProperties()));
        }

        //ALTER TABLE tool ADD ID INT UNSIGNED
        /// <summary>
        /// If mysqltable attribute is used and the columnname is defined, then this columnname will be returned. if not defined the name of the property will be returned.
        /// If neither of the properties use the mysqltable attribute, the name of the property will be returned, else exception will be thrown
        /// </summary>
        public static string GetPropertyDatabaseColumnName(System.Reflection.PropertyInfo property, System.Reflection.PropertyInfo[] properties)
        {
            if (GetUsesProperties(properties))
            {
                if (GetHasProperty(property))
                {
                    if (string.IsNullOrEmpty(((MySqlColumnAttribute)property.GetCustomAttributes(false).First(n => n.GetType() == typeof(MySqlColumnAttribute))).DatabaseColumnName))
                        return property.Name;
                    else
                        return ((MySqlColumnAttribute)property.GetCustomAttributes(false).First(n => n.GetType() == typeof(MySqlColumnAttribute))).DatabaseColumnName;
                }
                else
                    throw new Exception("This object uses property attributes but not this property");
            }
            else
                return property.Name;
        }

        /// <summary>
        /// True is always returned when neither of the properties use the mysqltable attribute, else returnes whatever the property attribute ReadFromDatabase field is set to. If no attribute, false is returned
        /// </summary>
        public static bool GetPropertyShouldRead(System.Reflection.PropertyInfo property, System.Reflection.PropertyInfo[] properties)
        {
            if (GetUsesProperties(properties))
            {
                if (GetHasProperty(property))
                    return ((MySqlColumnAttribute)property.GetCustomAttributes(false).First(n => n.GetType() == typeof(MySqlColumnAttribute))).ReadFromDatabase;
                else
                    return false;
            }
            else
                return true;
        }

        /// <summary>
        /// True is always returned when neither of the properties use the mysqltable attribute, else returnes whatever the property attribute WriteToDatabase field is set to. If no attribute, false is returned
        /// </summary>
        public static bool GetPropertyShouldWrite(System.Reflection.PropertyInfo property, System.Reflection.PropertyInfo[] properties)
        {
            if (GetUsesProperties(properties))
            {
                if (GetHasProperty(property))
                    return ((MySqlColumnAttribute)property.GetCustomAttributes(false).First(n => n.GetType() == typeof(MySqlColumnAttribute))).WriteToDatabase;
                else
                    return false;
            }
            else
                return true;
        }

        /// <summary>
        /// Returns the names of all properties to be read from the database
        /// </summary>
        public static string[] GetReadColumnNames(Type t)
        {
            return t.GetProperties().Where(n => GetPropertyShouldRead(n, t.GetProperties())).Select(n => GetPropertyDatabaseColumnName(n, t.GetProperties())).ToArray();
        }

        /// <summary>
        /// Returns wheter or not any of the properties uses the mysqltable attribute
        /// </summary>
        public static bool GetUsesProperties(System.Reflection.PropertyInfo[] properties)
        {
            return properties.Any(n => GetHasProperty(n));
        }

        /// <summary>
        /// Returns the names of all properties to be written to the database
        /// </summary>
        public static string[] GetWriteColumnNames(Type t)
        {
            return t.GetProperties().Where(n => GetPropertyShouldWrite(n, t.GetProperties())).Select(n => GetPropertyDatabaseColumnName(n, t.GetProperties())).ToArray();
        }

        /// <summary>
        /// Loads data from DataRow into instance
        /// </summary>
        public static void LoadDataRowIntoGeneric<T>(System.Data.DataRow row, T t, bool parse) where T : new()
        {
            foreach (System.Reflection.PropertyInfo property in t.GetType().GetProperties().Where(n => GetPropertyShouldRead(n, t.GetType().GetProperties())))
            {
                string columnName = GetPropertyDatabaseColumnName(property, t.GetType().GetProperties());

                if (!DBNull.Value.Equals(row[columnName]))
                    property.SetValue(t, parse ? Misc.Parsing.ParseObject(property.PropertyType, row[columnName]) : row[columnName], null);
            }
        }

        public static void LoadDataRowIntoGeneric(System.Data.DataRow row, object t, bool parse)
        {
            foreach (System.Reflection.PropertyInfo property in t.GetType().GetProperties().Where(n => GetPropertyShouldRead(n, t.GetType().GetProperties())))
            {
                string columnName = GetPropertyDatabaseColumnName(property, t.GetType().GetProperties());

                if (!DBNull.Value.Equals(row[columnName]))
                    property.SetValue(t, parse ? Misc.Parsing.ParseObject(property.PropertyType, row[columnName]) : row[columnName], null);
            }
        }

        #endregion Methods
    }
}