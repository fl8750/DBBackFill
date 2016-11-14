using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;


namespace DBBackfill
{

    public class ExtraSrcColumn
    {
        public TableInfo Table = null;
        public TableColInfo CopyColumn = null;
        public Dictionary<string, string> JoinColumns = new Dictionary<string, string>();

        public ExtraSrcColumn(TableInfo srcTable, string copyColumn, string joinColumns = "")
        {
            Table = srcTable;
            CopyColumn = Table[copyColumn];

        }
    }

    public class BackfillCtl 
    {
        //[Flags]
        //public enum BackFillOperation
        //{
        //    InsertMissing = 1,
        //    UpdateExisting = 2
        //}

        //  Private 
        //
        private readonly InstanceInfoList _instances = new InstanceInfoList();

        private StreamWriter logFile = null;

        //  General information
        //
        public string WorkSchemaName { get; private set; }
        public string SessionName { get; set; }
        public int CommandTimeout { get; set; } // Default SQL timeout in seconds

        //  Debug information
        //
        public Exception CapturedException = null;

        public int Debug { get; set; }
        public bool DebugToConsole { get; set; }
        private string _debugFile = string.Empty;   // Path to debug file, if one is required

        public string DebugFile
        {
            get { return _debugFile; }
            set
            {
                if (!string.IsNullOrEmpty(value))
                {
                    if (Debug <= 0) Debug = 1; // If debugging is off, turn it on
                }
                else
                {
                    if (!DebugToConsole) Debug = 0; // Turn debugging off if DebugToConsole is also off
                }
                _debugFile = value;
            }
        }

        public InstanceInfo this [string instanceName]
        { get { return _instances[instanceName]; } }


        //  Methods 
        //
        public void OpenInstance(string instanceName)
        {
            InstanceInfo newInst = _instances[instanceName];
            if (newInst == null)
            {
                newInst = _instances.OpenInstance(instanceName);
            }
        }


        public void DebugOutput(string debugMessage)
        {
            string strNow = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss.fff");
            string strDMsg = string.Format("{0} {1}: {2}", SessionName, strNow, debugMessage);
            
            if (DebugToConsole) Console.WriteLine(strDMsg);

            if (Debug > 0)
            {
                if (logFile == null)
                {
                    logFile = File.AppendText(DebugFile);
                }
                logFile.WriteLine(strDMsg);
                logFile.Close();
                logFile.Dispose();
                logFile = null;
            }
        }


        public void BackfillData(TableInfo srcTable, TableInfo dstTable, List<string> copyColNames,
                                 FetchKeyBoundary fkb, int batchSize,
                                 List<string> srcKeyNames = null, List<string> dstKeyNames = null)
        {
            BackfillContext bfCtx = new BackfillContext(this, srcTable, dstTable, copyColNames);
            if (fkb == null)
            {
                fkb = srcTable.CreateFetchKeyComplete(srcKeyNames);  // If no FetchKey specified, assume a full table scan
            }
            bfCtx.BackfillData(fkb, batchSize, srcKeyNames ?? fkb.FKeyColNames, dstKeyNames);
            bfCtx.Dispose();
        }

        public void BackfillData(TableInfo srcTable, TableInfo dstTable, List<string> copyColNames,
                                 FetchKeyBoundary fkb, int batchSize,
                                 string srcKeyNames , string dstKeyNames )
        {
            BackfillData(srcTable, dstTable, copyColNames, fkb, batchSize,
                           srcKeyNames.Split(',').ToList(), dstKeyNames.Split(',').ToList());
        }

        //  Method
        //
        public InstanceInfo GetInstance(string instanceName)
        {
            return _instances[instanceName];
        }

        //  Methods -- Database connections
        //
        static public SqlConnection OpenDB(string dbServer, string dbDbName = "master")
        {
            string connString = string.Format("server={0};database={1};trusted_connection=true;", dbServer, dbDbName);
            SqlConnection dbConn = new SqlConnection(connString);
            dbConn.Open();         
            return dbConn;
        }

        static public void CloseDb(SqlConnection dbConn)
        {
            if (dbConn != null)
            {
                if (dbConn.State != ConnectionState.Closed)
                    dbConn.Close();
            }
        }

        //
        //  Constructor
        //
        public BackfillCtl(string sessionName = "default", int debug = 0)
        {
            WorkSchemaName = "Backfill";
            SessionName = (sessionName == "default") ? DateTime.Now.ToString("yyyyMMddHHmm") : sessionName;
            Debug = debug;
            DebugToConsole = true;
            CommandTimeout = 600;
        }

        public BackfillCtl() {}
    }

}
