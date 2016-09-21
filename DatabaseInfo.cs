using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;


namespace DBBackfill
{

    public class DatabaseInfo : IEnumerable<TableInfo>
    {
        public string DatabaseName { get; private set; }
        public string InstanceName { get; private set;}
        public bool Locked { get; set; }

        public bool IsReady { get; set; } // True if information loaded

        public PublicationList Publications = null;

        private TableInfoList _tables = null;

        //  Indexers
        //
        public TableInfo this[int objectID]
        { get { return _tables[objectID]; } }

        public TableInfo this[string schemaName, string tableName]
        { get { return _tables[schemaName, tableName]; } }

        //  Method
        //
        public TableInfo GetTable(string schemaName, string tableName)
        {
            return this[schemaName, tableName];
        }

        public TableInfo GetTable(int objectID)
        {
            return this[objectID];
        }


        //  Ienumerable
        //
        public IEnumerator<TableInfo> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        //  Lazy initialization if the DatabaseInfo object
        //
        public void PrepareDatabaseInfo()
        {
            using (SqlConnection dbConn = BackfillCtl.OpenDB(InstanceName, DatabaseName))
            {
                _tables = new TableInfoList(dbConn, DatabaseName);
                Publications = new PublicationList(dbConn, this);
            }
            IsReady = true;
        }

        //  Constructors
        //
        public DatabaseInfo(SqlConnection dbConn, string dbName)
        {
            DatabaseName = dbName;
            InstanceName = dbConn.DataSource;
            Locked = true;
            IsReady = false;
        }
    }


    public class DatabaseInfoList : IEnumerable<DatabaseInfo>
    {
        private Dictionary<string, DatabaseInfo> _databases = new Dictionary<string, DatabaseInfo>(StringComparer.InvariantCultureIgnoreCase);

        //  Indexers
        //
        public DatabaseInfo this[string databaseName]
        {
            get
            {
                if (!_databases.ContainsKey(databaseName)) return null;
                DatabaseInfo dbEntry = _databases[databaseName];
                if (!dbEntry.IsReady) dbEntry.PrepareDatabaseInfo();
                return dbEntry;
            }
        }

        //  Enumerators
        //  
        public IEnumerator<DatabaseInfo> GetEnumerator()
        {
            return _databases.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _databases.Values.GetEnumerator();
        }

        //  Constructors
        //
        public DatabaseInfoList(SqlConnection dbConn)
        {

            //
            string strDatabaseList = @" 
        SELECT SDB.name
	        FROM sys.databases SDB
		        INNER JOIN sys.database_mirroring SDBM
			        ON (SDB.database_id = SDBM.database_id)
                
	        WHERE (SDB.state_desc = 'ONLINE')  AND (SDB.name <> 'model')
		        AND ((SDBM.mirroring_guid IS NULL) OR ((SDBM.mirroring_guid IS NOT NULL) AND (SDBM.mirroring_role_desc = 'PRINCIPAL')))
	        ORDER BY SDB.name;";

            try
            {
                DataTable dtDbList = new DataTable();
                using (SqlCommand cmdArtcol = new SqlCommand(strDatabaseList, dbConn))
                {
                    SqlDataReader srcRdr = cmdArtcol.ExecuteReader();
                    dtDbList.Load(srcRdr);
                    srcRdr.Close();
                }

                foreach (DataRow dbr in dtDbList.Rows)
                {
                    string dbName = (string)dbr["name"];
                    DatabaseInfo dbInfo = new DatabaseInfo(dbConn, dbName);
                    _databases.Add(dbName, dbInfo);
                }
            }
            catch (Exception)
            {
                throw;
            }

        }
    }
}
