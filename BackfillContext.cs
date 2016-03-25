﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;


namespace DBBackfill
{
    public partial class BackfillContext : IDisposable
    {
        //  Global information
        //
        public BackfillCtl BkfCtrl { get; private set; }

        //  Data source information
        //
        public SqlConnection SrcConn;
        public TableInfo SrcTable { get; private set; }
        public List<string> SrcKeyNames = new List<string>();
        public List<string> CopyColNames = new List<string>();

        //  Data destination Information
        //
        public SqlConnection DstConn;
        public TableInfo DstTable { get; private set; }
        public List<string> DstKeyNames = new List<string>();

        //  Work totals
        //
        public int FetchLoopCount = 0; // Number of completed fetchs
        public Int64 FetchRowCount = 0; // Total number of rows feched
        public Int64 MergeRowCount = 0; // Total number of rows inserted into the dest table

        //  Standard Query text
        //
        public string QryDataFetch = String.Empty; // Query to fetch data from the source table
        public string QryDataMerge = String.Empty; // Query to merge data from the temp table to the destination table

        //
        //  Work temp table properties
        //
        public string TempSchemaName
        {
            get { return BkfCtrl.WorkSchemaName; }
        }

        public string DstTempTableName
        {
            get { return string.Format("#{0}_{1}_{2}_{3}_{4}", BkfCtrl.SessionName, SrcTable.InstanceName, SrcTable.DbName, SrcTable.SchemaName, SrcTable.TableName); }
        }

        public string DstTempFullTableName
        {
            get { return string.Format("[{0}]", DstTempTableName); }
        }


        //
        //  Constructor and Disposal 
        //
        public BackfillContext(BackfillCtl bkfCtrl, TableInfo srcTable, TableInfo dstTable, List<string> copyColNames = null)
        {
            //  Save the input table info
            //
            BkfCtrl = bkfCtrl;

            SrcTable = srcTable;
            SrcConn = BackfillCtl.OpenDB(SrcTable.InstanceName, SrcTable.DbName);

            DstTable = dstTable;
            DstConn = BackfillCtl.OpenDB(DstTable.InstanceName, DstTable.DbName);

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
                throw new ApplicationException("Some specified columns do not exist");

            CopyColNames = copyColNames;

            //  Identify the default columns used for unique row indexing
            //
            SrcKeyNames = SrcTable.Where(cl => (cl.KeyOrdinal > 0)).OrderBy<TableColInfo, int>(cl => cl.KeyOrdinal).Select(cl => cl.Name).ToList();
            DstKeyNames = DstTable.Where(cl => (cl.KeyOrdinal > 0)).OrderBy(cl => cl.KeyOrdinal).Select(cl => cl.Name).ToList();
        }


        //  Dispose -- Close database connections and other cleanup tasks
        //
        public void Dispose()
        {
            BackfillCtl.CloseDb(SrcConn);
            SrcConn = null;
            BackfillCtl.CloseDb(DstConn);
            DstConn = null;
        }
    }
}