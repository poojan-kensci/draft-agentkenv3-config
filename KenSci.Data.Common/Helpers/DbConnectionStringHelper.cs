using System.Configuration;
using KenSci.Data.Common.Singletons;

namespace KenSci.Data.Common.Helpers
{
    public class DbConnectionStringHelper
    {
        public static string GetConfigConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["ConfigConnectionString"].ConnectionString;
        }

        public static string GetSourceConnectionString()
        {
            return (string) ConnectionsCache.Instance.GetOrNull("sourceConnectionString");
        }

        public static string GetSqlConnectionString(string server, string db, string userId, string password)
        {
            return
                $"Data Source={server};Initial Catalog={db};User ID={userId};Password={password};Connection Timeout=3600";
            return ConfigurationManager.ConnectionStrings["SourceConnectionString"].ConnectionString;
        }

        public static string GetDestinationConnectionString()
        {
            return (string) ConnectionsCache.Instance.GetOrNull("destinationConnectionString");
        }

        public static string GetOracleConnectionString(string server, string db, string userId, string password)
        {
            return $"USER ID={userId};DATA SOURCE={server}/{db};PASSWORD={password}";
        }
    }
}
