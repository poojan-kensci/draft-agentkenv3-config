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
                // columnRestrictions[1] = tableSchema;
                // columnRestrictions[1] = tableName;
                // columnRestrictions[1] = "DATA_ITEMS";
                columnRestrictions[1] = tableName.ToUpper();

                Console.WriteLine("columnRestrictions");
                Console.WriteLine(columnRestrictions[0]);
                Console.WriteLine(columnRestrictions[1]);
                Console.WriteLine(columnRestrictions[2]);

                DataTable sourceTableSchemaTable = sourceConnection.GetSchema("Columns", columnRestrictions);
                // DataTable sourceTableSchemaTable = sourceConnection.GetSchema("Columns");

                // for (int i = 0; i < sourceTableSchemaTable.Rows.Count; i++)
                // {
                //    Console.WriteLine($"{i}. {sourceTableSchemaTable.Rows[0].}");
                // }

                Console.WriteLine("sourceTableSchemaTable.Columns.Count: ");
                for (int i = 0; i < sourceTableSchemaTable.Columns.Count; i++)
                {
                    Console.WriteLine($"{i}. {sourceTableSchemaTable.Columns[i]}");
                }
                /*
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
                // Console.WriteLine(sourceTableSchemaTable.Columns[1]);
                // Console.WriteLine(sourceTableSchemaTable.Columns[2]);
                // Console.WriteLine(sourceTableSchemaTable.Columns[3]);
                // Console.WriteLine(sourceTableSchemaTable.Columns[4]);
                /*
                foreach (DataRow row in sourceTableSchemaTable.Rows)
                {
                   // Console.WriteLine(row.Table.TableName);
                   Console.WriteLine(row.Table.Columns[0]);
                }
                */
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
                $"Data Source={destinationServer};Initial Catalog={destinationDb};User ID=sa;Password=Pass123!;Connection Timeout=3000";

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

            Console.WriteLine("sourceTableSchemaTable.Rows.Count");
            Console.WriteLine(sourceTableSchemaTable.Rows.Count);

            Dictionary<string, string> dataTypeAdapterDict = new Dictionary<string, string>
            {
                { "NUMBER", "INTEGER" },
                { "VARCHAR2", "VARCHAR" },
            };

            var selectedRows = from info in sourceTableSchemaTable.AsEnumerable()
                select new
                {
                    // TableCatalog = info["TABLE_CATALOG"],
                    // TableSchema = info["TABLE_SCHEMA"],
                    // TableName = info["TABLE_NAME"],
                    // ColumnName = info["COLUMN_NAME"],
                    // DataType = info["DATA_TYPE"],
                    // CharacterMaximumLength = info["CHARACTER_MAXIMUM_LENGTH"],
                    ColumnName = info["COLUMN_NAME"],
                    // DataType = info["DATATYPE"],
                    DataType = dataTypeAdapterDict[info["DATATYPE"].ToString()],
                    CharacterMaximumLength = info["LENGTH"],
                };

            Console.WriteLine("POST selectedRows");
            Console.WriteLine(selectedRows.Count());


            foreach (var row in selectedRows)
            {
                // Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}{5,-15}", row.TableCatalog,
                //     row.TableSchema, row.TableName, row.ColumnName, row.DataType, row.CharacterMaximumLength);
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
