using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using KenSci.Data.Common.Helpers;
using Oracle.ManagedDataAccess.Client;

namespace KenSci.Data.Common.Engines
{
    public class OracleDataTransferEngine
    {
        private string GetOracleConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        }
        
        private string GetSqlConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
        }

        private DataTable GetOracleData()
        {
            string oracleConnectionString = GetOracleConnectionString();
            DataSet returnDataset = new DataSet();

            using (OracleConnection oracleConnection = new OracleConnection(oracleConnectionString))
            {
                LogHelper.Logger.Info("Connecting to Oracle... ");
                oracleConnection.Open();
                LogHelper.Logger.Info("Oracle connected.");
                
                string sql = "SELECT id, name, " +
                             "col1, col2, col3, col4, col5, col6, col7, col8, col9, " +
                             "col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, " +
                             "col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, " +
                             "col30, col31, col32, col33, col34, col35, col36, col37, col38, col39, " +
                             "col40, col41, col42, col43, col44, col45, col46, col47, col48, col49, " +
                             "col50, col51, col52, col53, col54, col55, col56, col57, col58, col59, " +
                             "col60, col61, col62, col63, col64, col65, col66, col67, col68, col69, " +
                             "col70, col71, col72, col73, col74, col75, col76, col77, col78, col79, " +
                             "col80, col81, col82, col83, col84, col85, col86, col87, col88, col89, " +
                             "col90, col91, col92, col93, col94, col95, col96, col97, col98, col99, " +
                             "col100 " +
                             "FROM data_items";

                using (OracleCommand command = new OracleCommand(sql, oracleConnection))
                {
                    command.CommandType = CommandType.Text;
                    var dataAdapter = new OracleDataAdapter(command);
                    dataAdapter.Fill(returnDataset);
                }
                             
            }

            return returnDataset.Tables[0];
        }

        private void InsertSqlData(DataTable dt)
        {
            string sql = "truncate table data_items;" + Environment.NewLine;
            string sqlConnectionString = GetSqlConnectionString();

            using (var connection = new SqlConnection(sqlConnectionString))
            {
                LogHelper.Logger.Info("Connecting to SQL Server... ");
                connection.Open();
                LogHelper.Logger.Info("SQL Server connected.");

                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandTimeout = 3600;
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.BulkCopyTimeout = 3600;
                    bulkCopy.DestinationTableName = "data_items";
                    bulkCopy.WriteToServer(dt);
                }
            }
            

        }
        public bool Import()
        {
            LogHelper.Logger.Info("Import started ... ");
            var config = ConfigurationManager.ConnectionStrings["OracleConnectionString"];
            LogHelper.Logger.Info(GetOracleConnectionString());
            LogHelper.Logger.Info(GetSqlConnectionString());

            var startTime = DateTime.Now;
            LogHelper.Logger.Info("Import started: Oracle -> SQL Server");

            var dt = GetOracleData();
            LogHelper.Logger.Info("Record Count: " + dt.Rows.Count.ToString());
            InsertSqlData(dt);
            LogHelper.Logger.Info("Import complete: Oracle -> SQL Server");

            int timeSpan = (DateTime.Now - startTime).Seconds;
            LogHelper.Logger.Info(timeSpan.ToString() + " Seconds");

            return true;
        }
    }
}