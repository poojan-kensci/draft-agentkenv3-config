using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using KenSci.Data.Common.Contracts;
using KenSci.Data.Common.Helpers;
using KenSci.Data.Common.Singletons;

namespace KenSci.Data.Common.Engines
{
    public class SqlDataTransfer : IDataTransfer
    {
        public DataTable FetchSourceSchema(
            string sourceDb,
            string tableSchema,
            string tableName
        )
        {
            LogHelper.Logger.Info("Fetching Source Table Schema ...");

            var sourceConnectionString = (string) ConnectionsCache.Instance.GetOrNull("sourceConnectionString");

            using (var sourceConnection = new SqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();
                String[] columnRestrictions = new String[4];
                columnRestrictions[0] = sourceDb;
                columnRestrictions[1] = tableSchema;
                columnRestrictions[2] = tableName;
                DataTable sourceTableSchemaTable = sourceConnection.GetSchema("Columns", columnRestrictions);
                LogHelper.Logger.Info("Fetching Source Table Schema complete.");
                return sourceTableSchemaTable;
            }
        }

        public void GenerateDestinationSchema(
            string tableName,
            string destinationSchema
        )
        {
            LogHelper.Logger.Info("Generating Destination Schema ...");

            var destinationConnectionString = (string) ConnectionsCache.Instance.GetOrNull("destinationConnectionString");

            var sqlCmd = new StringBuilder();
            sqlCmd.Append(
                $"if not exists (select * from sys.objects where object_id = OBJECT_ID(N'[{destinationSchema}].[{tableName}]') and type in (N'U')) {Environment.NewLine}");
            sqlCmd.Append($"create table {destinationSchema}.{tableName} ( {Environment.NewLine}");

            DataTable sourceTableSchemaTable = FetchSourceSchema(
                null, // unused for Oracle
                null, // unused for Oracle
                tableName
            );

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

            sqlCmd.Append(")");

            Console.WriteLine(sqlCmd.ToString());

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
