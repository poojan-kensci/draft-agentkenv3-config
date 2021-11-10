using System.Configuration;

namespace KenSci.Data.Common.Helpers
{
    public class DbConnectionStringHelper
    {
        public static string GetConfigConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["ConfigConnectionString"].ConnectionString;
        }
        
        // NOTE: This should be fetched from config database.
        public static string GetSourceConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SourceConnectionString"].ConnectionString;
        }
        
        // NOTE: This should be fetched from config database.
        public static string GetDestinationConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["DestinationConnectionString"].ConnectionString;
        }
        
        public static string GetDestinationConnectionString(string server, string db)
        {
            return ConfigurationManager.ConnectionStrings["DestinationConnectionString"].ConnectionString;
        }
    }
}