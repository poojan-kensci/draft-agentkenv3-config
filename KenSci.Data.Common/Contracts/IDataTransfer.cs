using System.Data;

namespace KenSci.Data.Common.Contracts
{
    public interface IDataTransfer
    {
        DataTable FetchSourceSchema(
            string sourceServer,
            string sourceDb,
            string tableSchema,
            string tableName
        );
        
        void GenerateDestinationSchema(
            string sourceServer,
            string sourceDb,
            string tableSchema,
            string tableName,
            string destinationServer,
            string destinationDb,
            string destinationSchema
        );
    }
}