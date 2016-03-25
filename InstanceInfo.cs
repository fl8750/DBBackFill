using System;
using System.Collections.Generic;
using System.Data.SqlClient;


namespace DBBackfill
{

    public class InstanceInfo : IEnumerable<DatabaseInfo>
    {
        public string InstanceName { get; private set; }
        private readonly DatabaseInfoList _databases = null;

        //  Indexers
        //
        public DatabaseInfo this[string databaseName]
        {
            get { return _databases[databaseName]; }
        }


        //  Method
        //
        public DatabaseInfo GetDatabase(string databaseName)
        {
            return _databases[databaseName];
        }

        //  Enumerators
        //
        public IEnumerator<DatabaseInfo> GetEnumerator()
        {
            return _databases.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _databases.GetEnumerator();
        }

        //  Constructors
        //
        public InstanceInfo(SqlConnection dbConn)
        {
            InstanceName = dbConn.DataSource;
            _databases = new DatabaseInfoList(dbConn);
        }
    }


    public class InstanceInfoList : IEnumerable<InstanceInfo>
    {
        private Dictionary<string, InstanceInfo> _instances = new Dictionary<string, InstanceInfo>(StringComparer.InvariantCultureIgnoreCase);

        //  Indexers
        //
        public InstanceInfo this[string instanceName]
        {
            get
            {
                if (!_instances.ContainsKey(instanceName)) return null;  // Return null if no instance open
                return _instances[instanceName];
            }
        }

        //  Methods
        //
        public InstanceInfo OpenInstance(string instanceName)
        {
            InstanceInfo newInst = null;
            try
            {
                using (SqlConnection dbConn = BackfillCtl.OpenDB(instanceName, "master"))
                {
                    newInst = new InstanceInfo(dbConn);
                    _instances.Add(instanceName, newInst);
                    BackfillCtl.CloseDb(dbConn);
                }
            }
            catch (Exception ex)
            {
                ;
            }
            return newInst;
        }

        //  Enumerators
        //  
        public IEnumerator<InstanceInfo> GetEnumerator()
        {
            return _instances.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _instances.Values.GetEnumerator();
        }

    }

}
