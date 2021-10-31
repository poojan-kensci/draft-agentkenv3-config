using System;
using System.Configuration;
using KenSci.Data.Common.Helpers;

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
        public void BulkCopy()
        {
            LogHelper.Logger.Info("Bulk Copy started ... ");
            var config = ConfigurationManager.ConnectionStrings["OracleConnectionString"];
            LogHelper.Logger.Info(GetOracleConnectionString());
            LogHelper.Logger.Info(GetSqlConnectionString());
        }
    }
}