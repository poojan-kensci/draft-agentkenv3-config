using System;
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

            var sqlConnectionString = engine.GetSqlConnectionString();

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