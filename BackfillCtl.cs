using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;


namespace DBBackfill
{

    public class BackfillCtl 
    {
        //  Private 
        //
        private readonly InstanceInfoList _instances = new InstanceInfoList();

        private StreamWriter logFile = null;

        //  General information properties
        //
        public string WorkSchemaName { get; private set; }
        public string SessionName { get; set; }
        public int ConnectTimeout { get; set; } // Default SQL connection attemp timeout in seconds
        public int CommandTimeout { get; set; } // Default SQL timeout in seconds

        public string Version
        {
            get { return System.Reflection.Assembly.GetAssembly(typeof(BackfillContext)).GetName().Version.ToString(); }
        }

        //  
        //  Lists
        //

        public InstanceInfo this[string instanceName]  // Indexer for the _instances list
        { get { return _instances[instanceName]; } }

        public InstanceInfo GetInstance(string instanceName)  // Added because PowerShell doesn't seem to handle indexers
        {
            return _instances[instanceName];
        }


        //  Debug properties
        //
        public Exception CapturedException = null;

        public int Debug { get; set; }
        public bool DebugToConsole { get; set; }
        private string _debugFile = string.Empty;   // Path to debug file, if one is required
 

        //
        //  Methods -- Database connections
        //
        static public SqlConnection OpenDB(string dbServer, int connectTimeout, string dbDbName = "master")
        {
            string connString = string.Format("server={0};database={1};Connection Timeout={2};trusted_connection=true", 
                dbServer, 
                dbDbName, 
                connectTimeout);
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
        //  Debug output Methods
        //
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


        // =======================================================================================
        //
        //  Methods 
        //
        // =======================================================================================

        public void OpenInstance(string instanceName)
        {
            InstanceInfo newInst = _instances[instanceName];
            if (newInst == null)
            {
                newInst = _instances.OpenInstance(instanceName);
            }
        }


        //
        //  Backfill overloaded methods
        //
        public void BackfillData(TableInfo srcTable,
                                 TableInfo dstTable,
                                 List<string> copyColNames,
                                 FetchKeyBoundary fkb,
                                 int batchSize,
                                 List<string> srcKeyNames = null,
                                 List<string> dstKeyNames = null)
        {
            BackfillContext bfCtx = new BackfillContext(this, srcTable, dstTable, copyColNames);
            if (fkb == null)
            {
                fkb = srcTable.CreateFetchKeyComplete(srcKeyNames);  // If no FetchKey specified, assume a full table scan
            }

            bfCtx.FillType = fkb.FillType;
            bfCtx.BackfillData(fkb, batchSize, bfCtx.BkfCtrl.CommandTimeout, srcKeyNames ?? fkb.FKeyColNames, dstKeyNames);
            bfCtx.Dispose();
        }

 
        public void BackfillData(FetchKeyBoundary fkb,
                                 TableInfo dstTable,
                                 int batchSize,
                                 List<string> srcKeyNames = null, List<string> dstKeyNames = null)
        {
            BackfillContext bfCtx = new BackfillContext(this, fkb.FKeySrcTable, dstTable);

            bfCtx.FillType = fkb.FillType;
            bfCtx.BackfillData(fkb, batchSize, bfCtx.BkfCtrl.CommandTimeout, fkb.FKeyColNames, dstKeyNames);
            bfCtx.Dispose();
        }

        public void BackfillData(TableInfo srcTable, TableInfo dstTable, List<string> copyColNames,
                                 FetchKeyBoundary fkb, int batchSize,
                                 string srcKeyNames, string dstKeyNames)
        {
            BackfillData(srcTable, dstTable, copyColNames, fkb, batchSize,
                           srcKeyNames.Split(',').ToList(), dstKeyNames.Split(',').ToList());
        }


        //
        //  Debug output file Methods
        //
        public void DebugOutput(string debugMessage)
        {
            string strNow = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
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

        //  Output debug information on the supplied Exception object
        //
        public void DebugOutputException(Exception ex)
        {
            Exception ex2 = ex;
            CapturedException = ex; // Save the exception information 

            for (int exNest = 0; ex2 != null; ++exNest)
            {
                DebugOutput(string.Format("Exception: [{0}] {1}", exNest, ex2.Message));
                DebugOutput(string.Format("Exception: [{0}] {1}", exNest, ex2.StackTrace));
                ex2 = ex2.InnerException;
            }
        }


        
        // =======================================================================================
        //
        //  Constructor
        //
        // =======================================================================================

        public BackfillCtl(string sessionName = "default", int debug = 0)
        {
            WorkSchemaName = "Backfill";
            SessionName = (sessionName == "default") ? DateTime.Now.ToString("yyyyMMddHHmm") : sessionName;
            Debug = debug;
            DebugToConsole = true;

            ConnectTimeout = 30;    // Set default timeouts
            CommandTimeout = 600;
        }

        public BackfillCtl() {}
    }

}
