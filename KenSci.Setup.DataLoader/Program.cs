using System;
using System.Data;
using System.Data.SqlClient;
using KenSci.Data.Common.Contracts;
using KenSci.Data.Common.Engines;
using KenSci.Data.Common.Helpers;
using KenSci.Data.Common.Singletons;

namespace KenSci.Setup.DataLoader
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var engine = new KenSci.Data.Common.Engines.OracleDataTransferEngine();

            var configDataRow = FetchConfigData();

            var sourceServer = configDataRow.Field<string>("SourceServer");
            var sourceDb = configDataRow.Field<string>("SourceDb");
            var tableSchema = configDataRow.Field<string>("TableSchema");
            var tableName = configDataRow.Field<string>("TableName");
            var destinationSchema = configDataRow.Field<string>("DestinationSchema");

            // Console.WriteLine(sourceServer);
            // Console.WriteLine(sourceDb);
            // Console.WriteLine(tableSchema);
            // Console.WriteLine(tableName);
            // Console.WriteLine(destinationSchema);

            // TODO: Get these from commandline parameters.
            var sourceUserId = "system";
            var sourcePassword = "OraPasswd1";
            var destinationUserId = "sa";
            var destinationPassword = "Pass123!";

            // TODO: Get this from config table or commandline parameters.
            // var destinationServer = "20.114.38.166";
            var destinationServer = "localhost";
            var destinationDb = "kensci1";

            var sourceConnectionString =
                DbConnectionStringHelper.GetOracleConnectionString(sourceServer, sourceDb, sourceUserId,
                    sourcePassword);
            var destinationConnectionString =
                DbConnectionStringHelper.GetSqlConnectionString(destinationServer, destinationDb, destinationUserId,
                    destinationPassword);

            var connectionsCache = ConnectionsCache.GetInstance;

            connectionsCache.Add("sourceConnectionString", sourceConnectionString);
            connectionsCache.Add("destinationConnectionString", destinationConnectionString);

            LogHelper.Logger.Info($"sourceConnectionString: {sourceConnectionString}");
            LogHelper.Logger.Info($"destinationConnectionString: {destinationConnectionString}");

            LogHelper.Logger.Info($"SourceServer: {sourceServer}, SourceDb: {sourceDb}");
            LogHelper.Logger.Info($"TableSchema: {tableSchema}, TableName: {tableName}");
            LogHelper.Logger.Info($"DestinationSchema: {destinationSchema}");

            // IDataTransfer dataTransfer = new SqlDataTransfer();
            IDataTransfer dataTransfer = new OracleDataTransfer();

            dataTransfer.GenerateDestinationSchema(
                sourceServer,
                sourceDb,
                tableSchema,
                tableName,
                destinationServer,
                destinationDb,
                destinationSchema
            );

            engine.Import();
        }

        private static DataRow FetchConfigData()
        {
            LogHelper.Logger.Info("Fetching Config Data ...");

            var configConnectionString = DbConnectionStringHelper.GetConfigConnectionString();

            using (var connection = new SqlConnection(configConnectionString))
            {
                string sql = "select * from KSAgent_Config_Tbl";
                using (var da = new SqlDataAdapter(sql, connection))
                {
                    var tblConfig = new DataTable();
                    connection.Open();
                    da.Fill(tblConfig);
                    var row = tblConfig.Rows[0];

                    LogHelper.Logger.Info("Fetching Config Data complete.");
                    return row;
                }
            }
        }
    }
}
