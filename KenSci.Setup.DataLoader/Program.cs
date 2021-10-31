using System;

namespace KenSci.Setup.DataLoader
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var engine = new KenSci.Data.Common.Engines.OracleDataTransferEngine();
            Console.WriteLine("Agent Ken V3");
            engine.BulkCopy();
        }
    }
}