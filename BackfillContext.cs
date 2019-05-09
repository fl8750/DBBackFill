using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;


namespace DBBackfill
{
    public enum BackfillType
    {
        BulkInsert = 1, // Bulk insert into dest table
        Merge, // Merge data into dest table3
        GapFill // Find gaps in the destination table and insert the proper source data row
    }

    public partial class BackfillContext : IDisposable
    {
        //  Global information
        //
        public BackfillCtl BkfCtrl { get; private set; }


        //  Execution control
        //
        public BackfillType FillType = BackfillType.Merge; // Backfill strategy
        //
        public int FetchLoopCount = 0; // Number of completed fetchs
        public Int64 FetchRowCount = 0; // Total number of rows feched
        public Int64 MergeRowCount = 0; // Total number of rows inserted into the dest table


        //  Data source information
        //
        public string SrcInstName { get; private set; } // Name of the source instance
        public string SrcDbName { get; private set; } // Name of the source database
        public string SrcSchemaName { get; private set; } // Name of the source schema
        public string SrcObjectName { get; private set; } // Name of the source table

        public string SrcPartitionFunc { get; private set; } // Name of the partition function controlling the source table
        public string SrcPartitionColName { get; private set; } // Source table column name used for partitioning 

        public string SrcAndWhere { get; set; } // Optional WHERE clause to be applied on each row fetch operation


        public TableInfo SrcTableInfo { get; private set; }
        public List<string> SrcKeyNames = new List<string>();
        public List<string> CopyColNames = new List<string>();


        //  Data destination Information
        //
        public string DstInstName { get; private set; } // Name of the destination instance
        public string DstDbName { get; private set; } // Name of the destination database
        public string DstSchemaName { get; private set; } // Name of the destination schema
        public string DstObjectName { get; private set; } // Name of the destination table        

        public TableInfo DstTableInfo { get; private set; }
        public List<string> DstKeyNames = new List<string>();


        //
        //  Properties
        //
        public bool IsSrcDstEqual
        {
            get { return ((SrcTableInfo.InstanceName == DstTableInfo.InstanceName) && (SrcTableInfo.DbName == DstTableInfo.DbName)); }
        }


        //  Standard Query text
        //
        public string QryKeyFetch { get; set; }  // Query to fetch key boundary for the subsequent row fetch from the source table
        public string QryDataFetch { get; set; }  // Query to fetch data from the source table
        public string QryDataMerge { get; set; }  // Query to merge data from the temp table to the destination table

        //
        //  Work temp table properties
        //
        public string TempSchemaName
        {
            get { return BkfCtrl.WorkSchemaName; }
        }

        public string DstTempTableName
        {
            get
            {
                return string.Format("{0}_{1}_{2}_{3}_{4}", BkfCtrl.SessionName, SrcTableInfo.InstanceName.Replace('\\', '_'), SrcTableInfo.DbName, SrcTableInfo.SchemaName, SrcTableInfo.TableName);
            }
        }

        public string DstTempFullTableName
        {
            get
            {
                return IsSrcDstEqual
                    ? string.Format("[#{0}]", DstTempTableName)
                    : string.Format("[{1}].[dbo].[{0}]", DstTempTableName, DstTableInfo.DbName);
            }
        }


        //
        //  Methods: Pre-Backfill Preparation
        //
        public void OpenSrcInst (string instName)
        {

        }


        //
        //  Constructor and Disposal 
        //
        public BackfillContext(BackfillCtl bkfCtrl, TableInfo srcTable, TableInfo dstTable, List<string> copyColNames = null)
        {
            //  Save the input table info
            //
            BkfCtrl = bkfCtrl;

            //SrcTable = srcTable;
            //SrcConn = BackfillCtl.OpenDB(SrcTable.InstanceName, SrcTable.DbName);

            //DstTable = dstTable;
            //DstConn = BackfillCtl.OpenDB(DstTable.InstanceName, DstTable.DbName);

            //  Get the list of columns to copy
            //
            if (copyColNames == null)
            {
                copyColNames = srcTable.Where(cc => (cc.IsCopyable)).Select(cc => cc.Name).ToList();
            }

            // Make sure all specified columns exist in the source and destination tables
            //
            int errCnt = 0;
            foreach (string ccName in copyColNames)
            {
                if (srcTable[ccName] == null)
                {
                    ++errCnt;
                    Console.WriteLine("Src: {0} - No column '{1}'", srcTable.FullTableName, ccName);
                    continue;
                }
                else if (!srcTable[ccName].IsCopyable)
                {
                    ++errCnt;
                    Console.WriteLine("Src: {0} - Column cannot be copied '{1}'", srcTable.FullTableName, ccName);
                    continue;
                }

                if (dstTable[ccName] == null)
                {
                    ++errCnt;
                    Console.WriteLine("Dst: {0} - No column '{1}'", srcTable.FullTableName, ccName);
                }
            }
            if (errCnt > 0)
                throw new ApplicationException("Some specified Copy columns do not exist");

            CopyColNames = copyColNames;

            //  Identify the default columns used for unique row indexing
            //
            SrcTableInfo = srcTable;
            DstTableInfo = dstTable;

            SrcKeyNames = srcTable.Where(cl => (cl.KeyOrdinal > 0)).OrderBy<TableColInfo, int>(cl => cl.KeyOrdinal).Select(cl => cl.Name).ToList();
            DstKeyNames = dstTable.Where(cl => (cl.KeyOrdinal > 0)).OrderBy(cl => cl.KeyOrdinal).Select(cl => cl.Name).ToList();

            //  Setup the initial copy/backfill options
            //
            FillType = BackfillType.BulkInsert; // Default to bulk insert
        }


        //  Dispose -- Close database connections and other cleanup tasks
        //
        public void Dispose()
        {
            //BackfillCtl.CloseDb(SrcConn);
            //SrcConn = null;
            //BackfillCtl.CloseDb(DstConn);
            //DstConn = null;
        }
    }
}
