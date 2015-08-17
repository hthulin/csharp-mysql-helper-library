namespace MySql.MysqlHelper
{
    /// <summary>
    /// Helper for generating connection string
    /// </summary>
    public class ConnectionString
    {
        #region Fields

        /// <summary>
        /// When true, multiple SQL statements can be sent with one command execution. Note: starting with MySQL 4.1.1, batch statements should be separated by the server-defined separator character. Statements sent to earlier versions of MySQL should be separated by ';'
        /// </summary>
        public bool allowBatch = true;

        /// <summary>
        /// Setting this to true indicates that the provider expects user variables in the SQL. This option was added in Connector/Net version 5.2.2.
        /// </summary>
        public bool allowUserVariables = true;

        /// <summary>
        /// If set to True, MySqlDataReader.GetValue() returns a MySqlDateTime object for date or datetime columns that have disallowed values, such as zero datetime values, and a System.DateTime object for valid values. If set to False (the default setting) it causes a System.DateTime object to be returned for all valid values and an exception to be thrown for disallowed values, such as zero datetime values.
        /// </summary>
        public bool allowZeroDatetime = false;

        /// <summary>
        /// Setting this option to true enables compression of packets exchanged between the client and the server. This exchange is defined by the MySQL client/server protocol. Compression is used if both client and server support ZLIB compression, and the client has requested compression using this option. A compressed packet header is: packet length (3 bytes), packet number (1 byte), and Uncompressed Packet Length (3 bytes). The Uncompressed Packet Length is the number of bytes in the original, uncompressed packet. If this is zero, the data in this packet has not been compressed. When the compression protocol is in use, either the client or the server may compress packets. However, compression will not occur if the compressed length is greater than the original length. Thus, some packets will contain compressed data while other packets will not.
        /// </summary>
        public bool compress = false;

        public int connectionAttemps = 3;

        public int connectionSleep = 50;

        /// <summary>
        /// The length of time (in seconds) to wait for a connection to the server before terminating the attempt and generating an error.
        /// </summary>
        public uint connectionTimeout = 5000;

        /// <summary>
        /// True to have MySqlDataReader.GetValue() and MySqlDataReader.GetDateTime() return DateTime.MinValue for date or datetime columns that have disallowed values.
        /// </summary>
        public bool convertZeroDateTime = true;

        /// <summary>
        /// Sets the default value of the command timeout to be used. This does not supersede the individual command timeout property on an individual command object. If you set the command timeout property, that will be used. This option was added in Connector/Net 5.1.4
        /// </summary>
        public uint defaultCommandTimeout = 5000;

        /// <summary>
        /// The password for the MySQL account being used.
        /// </summary>
        public string password;

        /// <summary>
        /// When true, the MySqlConnection object is drawn from the appropriate pool, or if necessary, is created and added to the appropriate pool. Recognized values are true, false, yes, and no.
        /// </summary>
        public bool pooling = true;

        /// <summary>
        /// The port MySQL is using to listen for connections. This value is ignored if Unix socket is used.
        /// </summary>
        public uint port = 3306;

        /// <summary>
        /// The name or network address of the instance of MySQL to which to connect. Multiple hosts can be specified separated by commas. This can be useful where multiple MySQL servers are configured for replication and you are not concerned about the precise server you are connecting to. No attempt is made by the provider to synchronize writes to the database, so take care when using this option. In Unix environment with Mono, this can be a fully qualified path to a MySQL socket file. With this configuration, the Unix socket is used instead of the TCP/IP socket. Currently, only a single socket name can be given, so accessing MySQL in a replicated environment using Unix sockets is not currently supported.
        /// </summary>
        public string server;

        /// <summary>
        /// The MySQL login account being used.
        /// </summary>
        public string username;

        #endregion Fields

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionString(string server, string uid, string pwd, uint port = 3306)
        {
            this.server = server;
            this.username = uid;
            this.password = pwd;
            this.port = port;
        }

        #endregion Constructors

        #region Methods

        /// <summary>
        /// ToString() override
        /// </summary>
        /// <returns>Returns a valid connection string</returns>
        public override string ToString()
        {
            return
            "Server=" + server +
            ";Port=" + port.ToString() +
            ";Uid=" + username.ToString() +
            ";Pwd=" + password.ToString() +
            ";AllowUserVariables=" + allowUserVariables.ToString() +
            ";ConnectionTimeout=" + connectionTimeout.ToString() +
            ";DefaultCommandTimeout=" + defaultCommandTimeout.ToString() +
            ";ConvertZeroDateTime=" + convertZeroDateTime.ToString() +
            ";Pooling=" + pooling.ToString() +
            ";Compress=" + compress.ToString() +
            ";AllowBatch=" + allowBatch.ToString() +
            ";AllowZeroDateTime=" + allowZeroDatetime.ToString();
        }

        #endregion Methods
    }
}