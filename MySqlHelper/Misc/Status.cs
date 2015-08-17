using System;

namespace MySql.MysqlHelper.Misc
{
    public class Status
    {
        #region Fields

        private double differenceSeconds = double.NaN;

        private XCon xCon = null;

        #endregion Fields

        #region Constructors

        public Status(XCon xCon)
        {
            this.xCon = xCon;
        }

        #endregion Constructors

        #region Methods

        /// <summary>
        /// Returns server timestamp. Only needs to query the database once.
        /// </summary>
        /// <returns></returns>
        public DateTime GetServerDateTime()
        {
            if (double.IsNaN(differenceSeconds))
                differenceSeconds = ((DateTime)xCon.GetObject("SELECT CURRENT_TIMESTAMP") - DateTime.Now).TotalSeconds;

            return DateTime.Now.AddSeconds(differenceSeconds);
        }

        #endregion Methods
    }
}