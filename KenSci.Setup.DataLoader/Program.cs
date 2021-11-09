using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using KenSci.Data.Common.Contracts;
using KenSci.Data.Common.Contracts.DTO;
using KenSci.Data.Common.Engines;
using KenSci.Data.Common.Helpers;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

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

            var destinationServer = sourceServer;
            // var destinationDb = sourceDb;
            var destinationDb = "kensci1";

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

            // engine.Import();
        }

        private static DataRow FetchConfigData()
        {
            LogHelper.Logger.Info("Fetching Config Data ...");
            var configConnectionString =
                "Data Source=localhost;Initial Catalog=AgentKenConfig;User ID=sa;Password=Pass123!;Connection Timeout=3000";

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