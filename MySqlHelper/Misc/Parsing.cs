using System;

namespace MySql.MysqlHelper.Misc
{
    public class Parsing
    {
        #region Methods

        public static object ParseObject(Type newType, object o)
        {
            if (newType == typeof(int))
                return Convert.ChangeType(int.Parse(o.ToString()), newType);

            if (newType == typeof(uint))
                return Convert.ChangeType(uint.Parse(o.ToString()), newType);

            if (newType == typeof(long))
                return Convert.ChangeType(long.Parse(o.ToString()), newType);

            if (newType == typeof(ulong))
                return Convert.ChangeType(ulong.Parse(o.ToString()), newType);

            if (newType == typeof(short))
                return Convert.ChangeType(short.Parse(o.ToString()), newType);

            if (newType == typeof(ushort))
                return Convert.ChangeType(ushort.Parse(o.ToString()), newType);

            if (newType == typeof(double))
                return Convert.ChangeType(double.Parse(o.ToString().Replace(',', '.')), newType, System.Globalization.CultureInfo.InvariantCulture);

            if (newType == typeof(float))
                return Convert.ChangeType(float.Parse(o.ToString().Replace(',', '.')), newType, System.Globalization.CultureInfo.InvariantCulture);

            if (newType == typeof(byte))
                return Convert.ChangeType(byte.Parse(o.ToString()), newType);

            if (newType == typeof(string))
                return Convert.ChangeType(o.ToString(), newType);

            if (newType == typeof(bool))
                return Convert.ChangeType(bool.Parse(o.ToString()), newType);

            if (newType == typeof(DateTime))
                return Convert.ChangeType(DateTime.Parse(o.ToString()), newType);

            if (newType.IsEnum)
                return Convert.ChangeType(Enum.Parse(newType, o.ToString()), newType);

            throw new Exception("No such type defined for parsing");
        }

        public static T ParseObject<T>(object o)
        {
            return (T)ParseObject(typeof(T), o);
        }

        #endregion Methods
    }
}