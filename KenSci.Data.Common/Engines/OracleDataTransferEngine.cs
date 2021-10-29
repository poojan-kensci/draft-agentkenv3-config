using System;
using System.Configuration;

namespace KenSci.Data.Common.Engines
{
    public class OracleDataTransferEngine
    {

        private string GetOracleConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        }
        
        private string GetSqlServerConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        }
        public void SayGoodbye()
        {
            Console.WriteLine("Say Goodbye!!");
            var config = ConfigurationManager.ConnectionStrings["OracleConnectionString"];
            // Console.WriteLine(GetOracleConnectionString());
            // Console.WriteLine(GetSqlServerConnectionString());

            Console.WriteLine(config);
        }
    }
}