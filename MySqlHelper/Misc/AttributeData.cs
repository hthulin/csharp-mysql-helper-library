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
        public bool WriteToDatabase = true;
        public bool ReadFromDatabase = true;
        public string DatabaseColumnName = null;
    }

    public class MysqlTableAttributeFunctions
    {
        /// <summary>
        /// Returns wheter or not any of the properties uses the mysqltable attribute
        /// </summary>
        public static bool GetUsesProperties(System.Reflection.PropertyInfo[] properties)
        {
            return properties.Any(n => GetHasProperty(n));
        }

        /// <summary>
        /// Returns wheter or not the property uses the mysqltable attribute
        /// </summary>
        public static bool GetHasProperty(System.Reflection.PropertyInfo property)
        {
            return property.GetCustomAttributes(false) != null && property.GetCustomAttributes(false).Any(n2 => n2.GetType() == typeof(MySqlColumnAttribute));
        }

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

        /// <summary>
        /// Returns the names of all properties to be read from the database
        /// </summary>
        public static string[] GetReadColumnNames(Type t)
        {
            return t.GetProperties().Where(n => GetPropertyShouldRead(n, t.GetProperties())).Select(n => GetPropertyDatabaseColumnName(n, t.GetProperties())).ToArray();
        }

        /// <summary>
        /// Returns the names of all properties to be written to the database
        /// </summary>
        public static string[] GetWriteColumnNames(Type t)
        {
            return t.GetProperties().Where(n => GetPropertyShouldWrite(n, t.GetProperties())).Select(n => GetPropertyDatabaseColumnName(n, t.GetProperties())).ToArray();
        }
    }
}
