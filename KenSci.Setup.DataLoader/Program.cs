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
        private static DataRow FetchConfigData()
        {
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

                    return row;
                }
            }
        }
        public static void Main(string[] args)
        {
            var engine = new KenSci.Data.Common.Engines.OracleDataTransferEngine();
            Console.WriteLine("Agent Ken V3");
            // engine.Import();

            var row = FetchConfigData();
            
            var sourceServer = row.Field<string>("SourceServer");
            var sourceDb = row.Field<string>("SourceDb");
            var tableSchema = row.Field<string>("TableSchema");
            var tableName = row.Field<string>("TableName");
            var destinationSchema = row.Field<string>("DestinationSchema");

            var destinationServer = sourceServer;
            // var destinationDb = sourceDb;
            var destinationDb = "kensci1";
            
            Console.WriteLine("SourceServer: {0}, SourceDb: {1}", sourceServer, sourceDb);
            Console.WriteLine("TableSchema: {0}, TableName: {1}", tableSchema, tableName);
            Console.WriteLine("DestinationSchema: {0}", destinationSchema);

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
                Console.WriteLine(sourceTableSchemaTable.Rows[0]);
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

                foreach (var row0 in selectedRows)
                {
                    Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}{5,-15}", row0.TableCatalog,
                        row0.TableSchema, row0.TableName, row0.ColumnName, row0.DataType, row0.CharacterMaximumLength);
                    sqlCmd.Append($"  {row0.ColumnName} {row0.DataType}({row0.CharacterMaximumLength}),{Environment.NewLine}");
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
               }
            }


            /*
            using (var connection = new SqlConnection(sqlConnectionString))
            {
                Console.WriteLine(connection);
                string sql = "select * from KSAgent_Config_Tbl";
                using (var da = new SqlDataAdapter(sql, connection))
                {
                    var tblConfig = new DataTable();
                    try
                    {
                        connection.Open();
                        da.Fill(tblConfig);
                        var row = tblConfig.Rows[0];
                        
                        var tableSchema = row.Field<string>("TableSchema");
                        var tableName = row.Field<string>("TableName");
                        var sourceDb = row.Field<string>("SourceDb");
                        var sourceServer = row.Field<string>("SourceServer");
                        var destinationSchema = row.Field<string>("DestinationSchema");
                        
                        Console.WriteLine("SourceServer: {0}, SourceDb: {1}", sourceServer, sourceDb);
                        Console.WriteLine("TableSchema: {0}, TableName: {1}", tableSchema, tableName);
                        Console.WriteLine("DestinationSchema: {0}", destinationSchema);
                        
                        
                    }
                    catch (Exception ex)
                    {
                        throw;
                    }
                }
            }
            
            */



            // ServerConnection serverConnection = new ServerConnection(sqlConnectionString);

            // var srv = new Server();
            // srv.ConnectionContext.ConnectionString = sqlConnectionString;

            // Database db = new Database();
            // string dbName = "kensci0";
            // db = srv.Databases[dbName];
            //
            // Console.WriteLine(db);

            // var smoEngine = new SQLManagementObjectEngine();
            // var scriptedSqlObjects = smoEngine.ScriptAllDatabaseTables(sqlConnectionString, "kensci0");
            // Console.WriteLine(scriptedSqlObjects.Count());
            // foreach (ScriptedSqlObject obj in scriptedSqlObjects)
            // {
            //     Console.WriteLine("obj.DatabaseName");
            //     // Console.WriteLine(obj.DatabaseName);
            // }

            // LogHelper.Logger.Info(scriptedDb);
            // Console.WriteLine(scriptedDb);
        }
    }
}