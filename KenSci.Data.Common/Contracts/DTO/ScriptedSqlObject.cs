using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KenSci.Data.Common.Contracts.DTO
{
    [Serializable]
    public class ScriptedSqlObject
    {
        public string CreateScript { get; set; }
        public string Schema { get; set; }
        public string DatabaseName { get; set; }
        public string ObjectName { get; set; }
        public string ObjectType { get; set; }
    }
}