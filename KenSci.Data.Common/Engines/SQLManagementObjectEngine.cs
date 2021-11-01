using KenSci.Data.Common.Contracts.Base;
using KenSci.Data.Common.Contracts.DTO;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace KenSci.Data.Common.Engines
{
    public class SQLManagementObjectEngine : LoggableComponent
    {
        Dictionary<string, IEnumerable<Contracts.DTO.ScriptedSqlObject>> _cachedObjects;

        internal const string cacheFileName = "kenscidatav2.bin";

        public SQLManagementObjectEngine()
        {
            _cachedObjects = new Dictionary<string, IEnumerable<ScriptedSqlObject>>();
            EvictCache();
        }
        public void EvictCache()
        {
            if (System.IO.File.Exists(cacheFileName))
            {
                try
                {
                    lock (_cachedObjects)
                    {
                        using (Stream stream = File.Open(cacheFileName, FileMode.Open))
                        {
                            // TODO identified risk: The application deserializes untrusted data without sufficiently verifying that the resulting data will be valid.
                            BinaryFormatter bin = new BinaryFormatter();
                            var cachedObjects = (Dictionary<string, IEnumerable<Contracts.DTO.ScriptedSqlObject>>)bin.Deserialize(stream);
                            _cachedObjects = cachedObjects;
                            _logger.Info($"Cache loaded ok! {_cachedObjects.Count}");
                            return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Couldn't restore cache of objects." + ex);
                }
            }

            _cachedObjects = new Dictionary<string, IEnumerable<Contracts.DTO.ScriptedSqlObject>>();
        }

        public void SaveCache()
        {
            try
            {
                lock (_cachedObjects)
                {
                    using (Stream stream = File.Open(cacheFileName, FileMode.Create))
                    {
                        BinaryFormatter bin = new BinaryFormatter();
                        bin.Serialize(stream, _cachedObjects);
                        stream.Flush();
                        stream.Close();
                    }
                }
            }
            catch (Exception ioe)
            {
                _logger.Error(ioe);
            }

        }

        public Contracts.DTO.ScriptedSqlObject ScriptSingleDatabaseObject(string _connectionString, string dbName, string tableName, string tableSchema = "dbo", string destinationSchema = "dbo")
        {
            ScriptedSqlObject singleObject = this.ScriptAllDatabaseTables(_connectionString, dbName, destinationSchema).FirstOrDefault(p => p.ObjectName == tableName);
            if(singleObject==null || singleObject.Schema != destinationSchema)
            {
                // Check and default schema just in case of null value in config database
                if (string.IsNullOrEmpty(tableSchema)) tableSchema = "dbo";
                singleObject = ScriptSingleObjectInternal(_connectionString, dbName, tableName, tableSchema, destinationSchema);
            }
            return singleObject;
        }

        private Contracts.DTO.ScriptedSqlObject ScriptSingleObjectInternal(
            string _connectionString, 
            string dbName, 
            string objectName, 
            string schema,
            string destinationSchema)
        {
            Contracts.DTO.ScriptedSqlObject scriptedSqlObject = new Contracts.DTO.ScriptedSqlObject();

            Server srv = new Server();
            srv.ConnectionContext.ConnectionString = _connectionString;

            Database db = new Database();
            db = srv.Databases[dbName];

            Table tbl = db.Tables[objectName, schema];
            View vw = db.Views[objectName, schema];
            Table destinationTable = new Table();
            string pkeyColumn = string.Empty;

            /* TODO: AH - May be possible to refactor all the if/else logic below to be cleaner.
             * This was coded under time constraints so putting it off for later.
             * This entire method probably needs a refactor.
             * */

            switch (tbl)
            {
                // Return if we don't find a source table or view
                case null when vw == null:
                    _logger.Info($"Alert! Couldn't export an object to warehouse {objectName}. This error may sometimes be safely ignored.");
                    return scriptedSqlObject;
                // If table is null, we have a view
                case null:
                {
                    // Clone view properties and destination schema
                    destinationTable.Parent = vw.Parent;
                    destinationTable.Name = vw.Name;
                    destinationTable.AnsiNullsStatus = vw.AnsiNullsStatus;

                    if (vw.Properties.Contains("DwMaterializedViewDistribution"))
                    {
                        destinationTable.DwTableDistribution = (DwTableDistributionType)vw.DwMaterializedViewDistribution;
                    }

                    var columns = vw.Columns;
                    foreach (Column col in columns)
                    {
                        Column newCol = new Column
                        {
                            Name = col.Name,
                            Parent = destinationTable,
                            DataType = col.DataType,
                            Collation = col.Collation,
                            Nullable = col.Nullable,
                        };

                        destinationTable.Columns.Add(newCol);
                    }

                    break;
                }
                default:
                {
                    // It's not a view and we found a source table so clone source table properties into destination table
                    destinationTable.Parent = tbl.Parent;
                    destinationTable.Name = tbl.Name;
                    destinationTable.AnsiNullsStatus = tbl.AnsiNullsStatus;
                    destinationTable.QuotedIdentifierStatus = tbl.QuotedIdentifierStatus;
                
                    // Clone column data to destination table
                    var columns = tbl.Columns;
                    foreach (Column col in columns)
                    {
                        var newCol = new Column
                        {
                            Name = col.Name,
                            Parent = destinationTable,
                            DataType = col.DataType,
                            Collation = col.Collation,
                            Nullable = col.Nullable,
                        };
                        destinationTable.Columns.Add(newCol);
                    }
                    
                    foreach (Index idx in tbl.Indexes)
                    {
                        if (idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
                        {
                            pkeyColumn = idx.IndexedColumns[0].Name;
                        }
                    } 
                    
                    break;
                }
            }

            try
            {
                // Set destination table schema to destination schema
                destinationTable.Schema = destinationSchema;
                
                StringBuilder sb = new StringBuilder();

                ScriptingOptions options = new ScriptingOptions();
                options.ClusteredIndexes = false;
                options.Default = true;
                options.DriAll = false;
                options.Indexes = false;
                options.IncludeHeaders = false;
                options.ScriptDataCompression = false;
                options.EnforceScriptingOptions = true;
                options.NoFileGroup = true;

                // Script using destination table instead of source table
                StringCollection coll = destinationTable.Script(options);
                foreach (string str in coll)
                {
                    sb.Append(str);
                    sb.Append(Environment.NewLine);
                }

                if (!string.IsNullOrEmpty(pkeyColumn))
                {
                    sb.Append($" WITH ( DISTRIBUTION = HASH([{pkeyColumn}]), CLUSTERED COLUMNSTORE INDEX)");
                }
                var tableScript = sb.ToString();
                _logger.Debug(tableScript);

                scriptedSqlObject = new Contracts.DTO.ScriptedSqlObject()
                {
                    CreateScript = tableScript,
                    DatabaseName = dbName,
                    ObjectName = destinationTable.Name,
                    Schema = destinationTable.Schema
                };
            }
            catch (Exception ex)
            {
                _logger.Info($"Alert! Couldn't export an object to warehouse {destinationTable.Name}. This error may sometimes be safely ignored.");
                _logger.Error(ex);
            }

            return scriptedSqlObject;
        }

        public IEnumerable<Contracts.DTO.ScriptedSqlObject> ScriptAllDatabaseTables(string _connectionString, string dbName, string destinationSchema = "dbo")
        {
            if (_cachedObjects.ContainsKey(dbName))
                return _cachedObjects[dbName];

            List<Contracts.DTO.ScriptedSqlObject> scriptedSqlObjects = new List<Contracts.DTO.ScriptedSqlObject>();
            string builder = string.Empty;

            Server srv = new Server();
            srv.ConnectionContext.ConnectionString = _connectionString;

            Database db = new Database();
            db = srv.Databases[dbName];

            foreach (Table tbl in db.Tables)
            {
                try
                {
                    // Create new table with destination schema
                    var destinationTable = new Table
                    {
                        Parent = tbl.Parent,
                        Name = tbl.Name,
                        AnsiNullsStatus = tbl.AnsiNullsStatus,
                        QuotedIdentifierStatus = tbl.QuotedIdentifierStatus,
                        Schema = destinationSchema
                    };
                    
                    // Clone column data to destination table
                    var columns = tbl.Columns;
                    foreach (Column col in columns)
                    {
                        var newCol = new Column
                        {
                            Name = col.Name,
                            Parent = destinationTable,
                            DataType = col.DataType,
                            Collation = col.Collation,
                            Nullable = col.Nullable,
                        };

                        destinationTable.Columns.Add(newCol);
                    }
                    
                    StringBuilder sb = new StringBuilder();
                    string tableScript = string.Empty;

                    ScriptingOptions options = new ScriptingOptions();
                    options.ClusteredIndexes = false;
                    options.Default = true;
                    options.DriAll = false;
                    options.Indexes = false;
                    options.IncludeHeaders = false;
                    options.ScriptDataCompression = false;
                    options.EnforceScriptingOptions = true;
                    options.NoFileGroup = true;

                    // Script using destination table instead of source table
                    StringCollection coll = destinationTable.Script(options);
                    foreach (string str in coll)
                    {
                        sb.Append(str);
                        sb.Append(Environment.NewLine);
                    }

                    //add hash indexes
                    string pkeyColumn = string.Empty;
                    foreach(Index idx in tbl.Indexes)
                    {
                       if(idx.IndexKeyType== IndexKeyType.DriPrimaryKey)
                       { 
                           pkeyColumn = idx.IndexedColumns[0].Name; 
                       }
                    }
                    if (!string.IsNullOrEmpty(pkeyColumn))
                    {
                        sb.Append($" WITH ( DISTRIBUTION = HASH([{pkeyColumn}]), CLUSTERED COLUMNSTORE INDEX)");
                    }
                    tableScript = sb.ToString();

                    _logger.Debug(tableScript);

                    scriptedSqlObjects.Add(new Contracts.DTO.ScriptedSqlObject()
                    {
                        CreateScript = tableScript,
                        DatabaseName = dbName,
                        ObjectName = destinationTable.Name,
                        Schema = destinationTable.Schema
                    });
                } catch (Exception ex)
                {
                    _logger.Info($"Alert! Couldn't export an object to warehouse {tbl.Name}. This error may sometimes be safely ignored.");
                    _logger.Error(ex);
                }
            }
            
            //cache the value 
            // TODO update cache with regenerated value
            _cachedObjects[dbName] = scriptedSqlObjects;
            SaveCache();
            return scriptedSqlObjects;

        }
    }
}
