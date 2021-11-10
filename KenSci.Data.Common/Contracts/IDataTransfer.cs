using System.Data;

namespace KenSci.Data.Common.Contracts
{
    public interface IDataTransfer
    {
        DataTable FetchSourceSchema(string sourceDb, string tableSchema, string tableName);
        
        void GenerateDestinationSchema(string tableName, string destinationSchema);
    }
}