using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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

            GenerateDestinationSchema(
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

        private static void GenerateDestinationSchema(
            string sourceServer,
            string sourceDb,
            string tableSchema,
            string tableName,
            string destinationServer,
            string destinationDb,
            string destinationSchema
            )
        {
            LogHelper.Logger.Info("Generating Destination Schema ...");
            var sourceConnectionString = $"Data Source={sourceServer};Initial Catalog={sourceDb};User ID=sa;Password=Pass123!;Connection Timeout=3000";
            var destinationConnectionString = $"Data Source={destinationServer};Initial Catalog={destinationDb};User ID=sa;Password=Pass123!;Connection Timeout=3000";
            
            var sqlCmd = new StringBuilder();
            sqlCmd.Append(
                $"if not exists (select * from sys.objects where object_id = OBJECT_ID(N'[{destinationSchema}].[{tableName}]') and type in (N'U'))");
            sqlCmd.Append($"create table {destinationSchema}.{tableName} ( {Environment.NewLine}");

            using (var sourceConnection = new SqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();
                String[] columnRestrictions = new String[4];
                columnRestrictions[0] = sourceDb;
                columnRestrictions[1] = tableSchema;
                columnRestrictions[2] = tableName;
                DataTable sourceTableSchemaTable = sourceConnection.GetSchema("Columns", columnRestrictions);
                var selectedRows = from info in sourceTableSchemaTable.AsEnumerable()
                    select new
                    {
                        TableCatalog = info["TABLE_CATALOG"],
                        TableSchema = info["TABLE_SCHEMA"],
                        TableName = info["TABLE_NAME"],
                        ColumnName = info["COLUMN_NAME"],
                        DataType = info["DATA_TYPE"],
                        CharacterMaximumLength = info["CHARACTER_MAXIMUM_LENGTH"],
                    };

                foreach (var row in selectedRows)
                {
                    Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}{5,-15}", row.TableCatalog,
                        row.TableSchema, row.TableName, row.ColumnName, row.DataType, row.CharacterMaximumLength);
                    sqlCmd.Append($"  {row.ColumnName} {row.DataType}({row.CharacterMaximumLength}),{Environment.NewLine}");
                }
            }
            
            sqlCmd.Append(")");
            
            Console.WriteLine(sqlCmd.ToString());

            Console.WriteLine(destinationConnectionString);

            using (var connection = new SqlConnection(destinationConnectionString))
            {
               connection.Open();
               using (var command = new SqlCommand(sqlCmd.ToString(), connection))
               {
                   command.CommandTimeout = 3600;
                   command.CommandType = CommandType.Text;
                   command.ExecuteNonQuery();

                   LogHelper.Logger.Info("Generating Destination Schema complete");
               }
            }
        }
    }
}