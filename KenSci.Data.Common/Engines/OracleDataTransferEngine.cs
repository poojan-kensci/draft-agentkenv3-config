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
        
        private string GetSqlConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
        }
        public void SayGoodbye()
        {
            Console.WriteLine("Say Goodbye!!");
            var config = ConfigurationManager.ConnectionStrings["OracleConnectionString"];
            Console.WriteLine(GetOracleConnectionString());
            Console.WriteLine(GetSqlConnectionString());

            Console.WriteLine(config);
        }
    }
}