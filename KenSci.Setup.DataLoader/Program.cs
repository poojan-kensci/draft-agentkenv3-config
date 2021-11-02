using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
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
            Console.WriteLine("Agent Ken V3");
            // engine.Import();

            // var sqlConnectionString = engine.GetSqlConnectionString();
            var sqlConnectionString =
                "Data Source=localhost;Initial Catalog=AgentKenConfig;User ID=sa;Password=Pass123!;Connection Timeout=3000";
            
            Console.WriteLine(sqlConnectionString);

            
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
                // var command = new SqlCommand(sql, connection);
                //     command.CommandTimeout = 3600;
                //     command.CommandType = CommandType.Text;
                //     var sqlDataReader = command.ExecuteReader();
                //
                //     while (sqlDataReader.Read())
                //     {
                //         Console.WriteLine(sqlDataReader[0]);
                //     }
                //     
                //     sqlDataReader.Close();
            }
            
            
            

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