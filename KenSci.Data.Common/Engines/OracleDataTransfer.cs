using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using KenSci.Data.Common.Contracts;
using KenSci.Data.Common.Helpers;
using System.Data.SqlClient;
using NLog;
using Oracle.ManagedDataAccess.Client;

namespace KenSci.Data.Common.Engines
{
    public class OracleDataTransfer : IDataTransfer
    {
        public DataTable FetchSourceSchema(string sourceServer, string sourceDb, string tableSchema, string tableName)
        {
            LogHelper.Logger.Info("Fetching Source Table Schema ...");

            var sourceConnectionString =
                $"USER ID=system;DATA SOURCE=20.114.38.166/oratest1;PASSWORD=OraPasswd1";

            using (var sourceConnection = new OracleConnection(sourceConnectionString))
            {
                sourceConnection.Open();
                String[] columnRestrictions = new String[3];
                // columnRestrictions[0] = sourceDb;
                columnRestrictions[1] = tableName.ToUpper();

                /*
                Oracle Columns:
                0. OWNER
                1. TABLE_NAME
                2. COLUMN_NAME
                3. ID
                4. DATATYPE
                5. LENGTH
                6. PRECISION
                7. SCALE
                8. NULLABLE
                9. CHAR_USED
                10. LENGTHINCHARS
                */
                
                DataTable sourceTableSchemaTable = sourceConnection.GetSchema("Columns", columnRestrictions);
                
                LogHelper.Logger.Info("Fetching Source Table Schema complete.");
                return sourceTableSchemaTable;
            }
        }

        public void GenerateDestinationSchema(
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

            var destinationConnectionString =
                $"Data Source={destinationServer};Initial Catalog={destinationDb};User ID=sa;Password=Pass123!;Connection Timeout=3600";

            var sqlCmd = new StringBuilder();
            sqlCmd.Append(
                $"if not exists (select * from sys.objects where object_id = OBJECT_ID(N'[{destinationSchema}].[{tableName}]') and type in (N'U')) {Environment.NewLine}");
            sqlCmd.Append($"create table {destinationSchema}.{tableName} ( {Environment.NewLine}");

            DataTable sourceTableSchemaTable = FetchSourceSchema(
                sourceServer,
                sourceDb,
                tableSchema,
                tableName
            );

            var noOfColumns = sourceTableSchemaTable.Rows.Count;
            LogHelper.Logger.Info($"No of Columns: {noOfColumns.ToString()}");

            // Oracle to SQL data type mapping
            Dictionary<string, string> dataTypeAdapterDict = new Dictionary<string, string>
            {
                { "NUMBER", "INTEGER" },
                { "VARCHAR2", "VARCHAR" },
            };

            var selectedRows = from info in sourceTableSchemaTable.AsEnumerable()
                select new
                {
                    ColumnName = info["COLUMN_NAME"],
                    DataType = dataTypeAdapterDict[info["DATATYPE"].ToString()],
                    CharacterMaximumLength = info["LENGTH"],
                };

            foreach (var row in selectedRows)
            {
                Console.WriteLine("{0,-15}{1,-15}{2,-15}",
                    row.ColumnName, row.DataType, row.CharacterMaximumLength);
                sqlCmd.Append($"  {row.ColumnName} {row.DataType}({row.CharacterMaximumLength}),{Environment.NewLine}");
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

                    LogHelper.Logger.Info("Generating Destination Schema complete.");
                }
            }
        }
    }
}
