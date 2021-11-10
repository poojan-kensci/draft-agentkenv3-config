using System.Configuration;

namespace KenSci.Data.Common.Helpers
{
    public class DbConnectionStringHelper
    {
        public static string GetOracleConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        }
        
        public static string GetSqlConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
        }
        
        // NOTE: This should be fetched from config database.
        public static string GetSourceConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SourceConnectionString"].ConnectionString;
        }
        
        // NOTE: This should be fetched from config database.
        public static string GetDestinationConnectionString(string server, string db)
        {
            return ConfigurationManager.ConnectionStrings["DestinationConnectionString"].ConnectionString;
        }
    }
}