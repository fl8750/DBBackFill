using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;


namespace DBBackfill
{

    public partial class BackfillContext
    {
        private string _sqlPartitionSizes = @"
                SELECT  SCH.name AS SchemaName ,
                        TAB.name AS TableName ,
                        SUM(PT.[rows]) OVER ( PARTITION BY PT.[object_id] ) AS TotalRows ,
                        PT.partition_number ,
                        PT.[rows]
                FROM    sys.tables TAB
                        INNER JOIN sys.schemas SCH ON ( TAB.[schema_id] = SCH.[schema_id] )
                        INNER JOIN sys.indexes IDX ON ( TAB.[object_id] = IDX.[object_id] )
                        INNER JOIN sys.partitions PT ON ( TAB.[object_id] = PT.[object_id] )
                                                        AND ( IDX.index_id = PT.index_id )
                WHERE   ( SCH.name = '{0}' )
                        AND ( TAB.name = '{1}' )
                        AND ( IDX.index_id IN ( 0, 1 ) )
                        AND ( PT.[rows] > 0 )
                ORDER BY SchemaName ,
                        TableName ,
                        partition_number";


        //  Worker properties
        //
        public void BackfillData(FetchKeyBoundary fkb,
                                 int batchSize,
                                 List<string> srcKeyNames,
                                 List<string> dstKeyNames = null
                                 )
        {
            //  Local information
            //
            bool isSameInstance = false;
            bool hasRestarted = false; // 

            int curFetchCount = 0; // Number of rows fetched from the source table
            int curMergeCount = 0; // Number of rows merged into the destination table

            Dictionary<int, Int64> PartSizesAll = new Dictionary<int, Int64>();    // Per partition row counts for all partitions.
            List<int> PartsNotEmpty = new List<int>();    // Partition # of non-empty partitions.


            //  Prepare the SQL connections to the source and destination databases
            //
            SqlConnection srcConn = BackfillCtl.OpenDB(SrcTable.InstanceName, SrcTable.DbName);
            SqlConnection dstConn = BackfillCtl.OpenDB(DstTable.InstanceName, DstTable.DbName);

            isSameInstance = (string.Compare(srcConn.DataSource, dstConn.DataSource, StringComparison.InvariantCultureIgnoreCase) == 0);

            //  Prepare the list of fetch key columns and merge key columns
            //
            if ((srcKeyNames == null) || (srcKeyNames.Count < 1))
            {
                srcKeyNames = fkb.FKeyColNames;
            }

            if ((dstKeyNames == null) || (dstKeyNames.Count < 1))
            {
                dstKeyNames = DstKeyNames;
            }

            //  Main code section -- start of the TRY / CATCH
            //
            try
            {
                //  Start 
                BkfCtrl.DebugOutput(string.Format("Backfill Start [{0}].[{1}].{2} --> [{3}].[{4}].{5}\n",
                        SrcTable.InstanceName,
                        SrcTable.DbName,
                        SrcTable.FullTableName,
                        DstTable.InstanceName,
                        DstTable.DbName,
                        DstTable.FullTableName
                       ));

                //  Get the partition sizing info if "partition selection" is required
                //
                using (SqlCommand cmdPtSz = new SqlCommand(string.Format(_sqlPartitionSizes, SrcTable.SchemaName, SrcTable.TableName), srcConn)) // Source database command
                {
                    cmdPtSz.CommandType = CommandType.Text;
                    cmdPtSz.CommandTimeout = 600;
                    using (SqlDataReader srcPtRdr = cmdPtSz.ExecuteReader()) // Fetch data into a data reader
                    {
                        using (DataTable srcDt = new DataTable())
                        {
                            srcDt.Load(srcPtRdr); // Load the data into a DataTable
                            PartSizesAll = srcDt.AsEnumerable().Select(p =>
                                new{
                                    PartNo = (int) p["partition_number"],
                                    Rows = (Int64) p["Rows"]
                                }).ToDictionary(pt => pt.PartNo, pt => pt.Rows);
                            SrcTable.RowCount = (Int64) srcDt.Rows[0]["TotalRows"];
                        }
                    }
                }
                PartsNotEmpty = PartSizesAll.Keys.Where(pi => (PartSizesAll[pi] > 0)).OrderBy(pi => pi).ToList(); // Record the partition numbers that are not empty 

                //  Show the rowcount
                BkfCtrl.DebugOutput(string.Format("      Rows: {0:#,##0}  BatchChunkSize: {1:#,##0}  Partitions(#/>0): {2:#,##0}/{3:#,##0}",
                        DstTable.RowCount,
                        batchSize,
                        PartSizesAll.Count, PartsNotEmpty.Count
                        ));

                if (FillType != BackfillType.BulkInsert)
                    PrepareWorker((IsSrcDstEqual) ? srcConn : dstConn, dstKeyNames); // Setup the needed SQL environment

                //  Reset data pump loop counters
                //
                FetchRowCount = 0; // Total number of rows feched
                MergeRowCount = 0; // Total number of rows inserted into the dest table

                int ptIdx = 0;
                if ( fkb.FlgRestart && (fkb.RestartPartition != 1))
                {
                    for (ptIdx = 0; ptIdx < PartsNotEmpty.Count; ptIdx++)
                        if (PartsNotEmpty[ptIdx] >= fkb.RestartPartition)
                            break; // If the selected partition does exists then start at the next highest or the last
                }

                //  Partition Loop -- Once per partition
                //
                for (; ptIdx < ((fkb.FlgSelectByPartition) ? PartsNotEmpty.Count : 1); ptIdx++)
                {
                    int curPartition = PartsNotEmpty[ptIdx];    // Current partition number
                    hasRestarted = true;  // Assume a restart at the start of each partition

                    //  
                    //  Setup the initial key value list
                    //
                    List<object> currentFKeyList = new List<object>();
                    if (hasRestarted && fkb.FlgRestart)
                    {
                        for (int idx = 0; (idx < fkb.RestartKeys.Count) && (idx < srcKeyNames.Count); idx++)
                        {
                            currentFKeyList.Add(fkb.RestartKeys[idx]); // Process any restart keys values
                        }
                        hasRestarted = false;
                    }
                    else
                    {
                        for (int idx = 0; (idx < fkb.StartKeyList.Count) && (idx < srcKeyNames.Count); idx++)
                        {
                            currentFKeyList.Add(fkb.StartKeyList[idx]); // Copy a key value              
                        }
                    }

                    //
                    //  Main Data Pump loop
                    //
                    FetchLoopCount = 0; // Number of completed fetchs
                    while (true)
                    {
                        //int partNo = PartsNotEmpty[ptIdx];
                        string sqlFetchData = "";
                        List<object> nextFKeyList;

                        SqlTransaction trnMerge;
                        //Dictionary<string, SqlParameter> fetchParamList = new Dictionary<string, SqlParameter>();

                        //  Build the SQL command to fetch the next set of rows
                        //
                        //fkb.BuildFetchQuery(SrcTable, fetchParamList, curPartition, FetchLoopCount, srcKeyNames, currentFKeyList);

                        //SqlConnection tmpConn = null;
                        //if (IsSrcDstEqual)
                        //if (FillType == BackfillType.BulkInsert)
                        //{
                        //    //  Build the final batch fetch SQL command
                        //    //
                        //    sqlFetchData = string.Format(
                        //        fkb.FetchLastSql,
                        //        batchSize,
                        //        string.Join(", \n           ", CopyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()),
                        //        DstTempFullTableName
                        //        ); // Complete the fetch query setup
                        //    tmpConn = dstConn;
                        //}

                        //else
                        //if (FillType == BackfillType.BulkInsert)
                        //    {
                        //    //  Build the final batch fetch SQL command
                        //    //
                        //    sqlFetchData = string.Format(
                        //        fkb.FetchBatchSql,
                        //        batchSize,
                        //        string.Join(", \n       ", CopyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray())
                        //        ); // Complete the fetch query setup
                        //    tmpConn = srcConn;
                        //}

                        //   Fetch the next set of rows into a DataTable
                        //
                        using (DataTable srcDt = new DataTable())
                        {
                            using (SqlCommand cmdSrcDb = new SqlCommand("", srcConn)) // Source database command
                            {
                                fkb.BuildFetchQuery(cmdSrcDb, SrcTable, batchSize, curPartition, (FetchLoopCount == 0), CopyColNames, srcKeyNames, currentFKeyList);

                                cmdSrcDb.CommandText = fkb.FetchBatchSql;
                                cmdSrcDb.CommandType = CommandType.Text;
                                cmdSrcDb.CommandTimeout = BkfCtrl.CommandTimeout;

                                using (SqlDataReader srcRdr = cmdSrcDb.ExecuteReader()) // Fetch data into a data reader
                                {
                                    srcDt.Load(srcRdr); // Load the data into a DataTable
                                    srcRdr.Close(); // Close the reader
                                }

                                //  Get the key values from the last row in the batch
                                //
                                curFetchCount = srcDt.Rows.Count;
                                if (curFetchCount <= 0) break; // Nothing fetched, exit the data pump loop
                                //if (IsSrcDstEqual)
                                //{
                                //    curFetchCount = (int) (Int64) srcDt.Rows[0]["__ROWS__"]; // Get the real fetch count
                                //    nextFKeyList = fkb.FetchNextKeyList(srcDt.Rows[0]); // Get the last row keys
                                //}
                                //else
                                nextFKeyList = fkb.FetchNextKeyList(srcDt.Rows[curFetchCount - 1]); // Get the last row key
                                FetchRowCount += curFetchCount;

                                ++FetchLoopCount; // Increment the loop count
                            }

                            //  Create a transaction object  - Protect the bulk import, merge and truncate statement in a transaction
                            //
                            if (curFetchCount > 0)
                            {
                                trnMerge = dstConn.BeginTransaction();
                                try
                                {
                                    //  Setup for the SqlBulk Insert to the destination temp table
                                    //
                                    curMergeCount = 0;
                                    if ((FillType == BackfillType.BulkInsert) || (FillType == BackfillType.BulkInsertMerge))
                                    {
                                        BulkInsertIntoTable(srcDt, trnMerge, dstConn, DstTable.FullTableName, CopyColNames);
                                        curMergeCount = srcDt.Rows.Count;
                                    }

                                    if ((FillType == BackfillType.Merge) || (FillType == BackfillType.BulkInsertMerge))
                                    {
                                        //BulkInsertIntoTable(srcDt, trnMerge, dstConn, string.Concat("tempdb..", DstTempFullTableName), CopyColNames);
                                        BulkInsertIntoTable(srcDt, trnMerge, dstConn, DstTempFullTableName, CopyColNames);
                                        using (SqlCommand cmdDstMerge = new SqlCommand(QryDataMerge, dstConn, trnMerge)) //  Destination datbase connection
                                        {
                                            cmdDstMerge.CommandTimeout = BkfCtrl.CommandTimeout; // Set the set timeout value
                                            using (DataTable dtMerge = new DataTable())
                                            {
                                                using (SqlDataReader mrgRdr = cmdDstMerge.ExecuteReader()) // Merge the bulk insert rows and return stats
                                                {
                                                    dtMerge.Load(mrgRdr); // Load the data into a DataTable
                                                    mrgRdr.Close(); // Close the reader
                                                }

                                                curMergeCount = (int)dtMerge.Rows[0]["_InsCount_"];
                                                int curDelCount = (int)dtMerge.Rows[0]["_DelCount_"];
                                            }
                                        }
                                    }

                                    MergeRowCount += curMergeCount;
                                    trnMerge.Commit();
                                }
                                catch (Exception ex)
                                {
                                    trnMerge.Rollback();
                                    throw new ApplicationException("BackfillWorkerLoop", ex);
                                }
                            }
                        }

                        //  Last step -- write a progress record for restart purposes
                        //
                        if (BkfCtrl.Debug > 0)
                        {
                            BkfCtrl.DebugOutput(string.Format("-- [{7}:{8}] F:{0:#,##0}/{1:#,##0}/{2:#,##0} M:{3:#,##0}/{4:#,##0} {5}/{6}",
                                curFetchCount,
                                FetchRowCount,
                                DstTable.RowCount,
                                curMergeCount,
                                MergeRowCount,
                                ((currentFKeyList != null) && (currentFKeyList.Count > 0)) ? currentFKeyList[0].ToString() : "---",
                                ((fkb.EndKeyList != null) && (fkb.EndKeyList.Count > 0)) ? fkb.EndKeyList[0].ToString() : "---",
                                curPartition,
                                FetchLoopCount
                                ));
                        }

                        currentFKeyList = nextFKeyList; // Set the current key list with the key values from last fetched row
                    }
                }

                //  Remove the temp table after we are finished with this table
                //
                using (SqlCommand cmdDstTruc = new SqlCommand(string.Format("DROP TABLE {0} ", DstTempFullTableName), dstConn)) //  Destination datbase connection
                {
                    cmdDstTruc.ExecuteNonQuery();
                }

                // Completion messages
                //

                if (BkfCtrl.Debug > 0)
                {
                    BkfCtrl.DebugOutput(string.Format("-- Backfill Complete --- Copied: {0:#,##0}  Merged: {1:#,##0}  Chunks: {2:#,##0} ",
                        FetchRowCount,
                        MergeRowCount, 
                        FetchLoopCount
                       ));
                }

                BkfCtrl.CapturedException = null;
            }
            catch (Exception ex)
            {
                BkfCtrl.CapturedException = ex; // Save the exception information 
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    Console.WriteLine(ex.InnerException.Message);
                    Console.WriteLine(ex.InnerException.StackTrace);
                }
                throw new ApplicationException("Worker Error: ", ex);
            }
            finally
            {
                BackfillCtl.CloseDb(srcConn);
                BackfillCtl.CloseDb(dstConn);
            }
        }



        // ===============================================================================================================================
        //
        //  Bulk Import a DataTable into a destination table
        //
        private int BulkInsertIntoTable(DataTable srcDt, SqlTransaction trnActive, SqlConnection dstConn, string destTableName, List<string> copyColNames)
        {
            SqlBulkCopyOptions bcpyOpts = (DstTable.HasIdentity)
                ? (SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls)
                : SqlBulkCopyOptions.KeepNulls;
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(dstConn, bcpyOpts, trnActive))
            {
                try
                {
                    bulkCopy.BulkCopyTimeout = BkfCtrl.CommandTimeout;
                    bulkCopy.DestinationTableName = destTableName;

                    // Setup explicit column mapping to avoid any problem from the default mapping behavior
                    //
                    foreach (string ccn in copyColNames)
                    {
                        TableColInfo tci = DstTable[ccn];
                        if (tci.IsIncluded)
                        {
                            SqlBulkCopyColumnMapping newCMap = new SqlBulkCopyColumnMapping(tci.Name, tci.Name);
                            bulkCopy.ColumnMappings.Add(newCMap);
                        }
                    }
                    bulkCopy.WriteToServer(srcDt); // Write from the source to the destination.
                    return srcDt.Rows.Count;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return 0;
                }
            }
        }



        // ===============================================================================================================================
        //
        //  Prepare the destination database, work table and query texts for the backfill operation
        //
        public void PrepareWorker(SqlConnection dstConn, List<string> dstKeyNames)
        {
            string strSavedDatabase = string.Empty;

            try
            {
                strSavedDatabase = dstConn.Database;    // Save the current database setting

                //  Create the destination work table if not there, or drop any existing table.
                //
                try
                {
                    string sqlTableDrop;
                    if (IsSrcDstEqual)
                    {
                        dstConn.ChangeDatabase("tempdb"); // Switch to the temp datababse
                        sqlTableDrop = string.Format(@"IF OBJECT_ID('tempdb..[{0}]') IS NOT NULL DROP TABLE [{0}]; ", DstTempTableName);
                        dstConn.ChangeDatabase(strSavedDatabase); // Switch to the temp datababse
                    }
                    else
                    {
                        sqlTableDrop = string.Format(@"IF OBJECT_ID('{0}') IS NOT NULL DROP TABLE {0}; ", DstTempFullTableName);
                    }
                    using (SqlCommand cmdWTab = new SqlCommand(sqlTableDrop, dstConn))
                    {
                        cmdWTab.ExecuteNonQuery();
                    }
                }
                finally {}

                //
                //  Create the temp import table used as input for the subsequent MERGE command
                //
                string strCreateWTab = @"
                                    SELECT *
                                        INTO {2}
                                        FROM [{0}].[{1}]
                                        WHERE 1=0;
                                    CREATE UNIQUE CLUSTERED INDEX [UCI_{3}_{4}] 
                                        ON {2} ({5});";
                                 //   {6} ALTER TABLE {2} ADD [__ROWS__] INT NOT NULL";

                string sqlCreateWTab = string.Format(strCreateWTab,
                    DstTable.DbName,
                    DstTable.FullTableName,
                    DstTempFullTableName,
                    TempSchemaName,
                    DstTempTableName,
                    string.Join(", ", dstKeyNames.Select(cn => string.Format("[{0}]", cn)).ToArray()),
                    (IsSrcDstEqual) ? "" : "--"
                    );

                using (SqlCommand cmdSch = new SqlCommand(sqlCreateWTab, dstConn))
                {
                    cmdSch.ExecuteNonQuery();
                }              
            }
            finally
            {
                dstConn.ChangeDatabase(strSavedDatabase);
            }
            
            //
            //  Create the matched record portion of the MERGE command
            //
            //StringBuilder sbMatched = new StringBuilder();

            //List<TableColInfo> testCols = DstTable.Where(tci => (CopyColNames.Contains(tci.Name))).Where(tci => (!dstKeyNames.Contains(tci.Name))).ToList();

            //if (testCols.Count > 0)
            //{
            //    sbMatched.AppendLine("WHEN MATCHED AND (");
            //    for (int idx = 0; idx < testCols.Count; idx++)
            //    {
            //        TableColInfo tci = testCols[idx];
            //        if ((!tci.IsComparable) || (tci.IsIdentity)) continue;
            //        sbMatched.Append("                                                  (");
            //        sbMatched.AppendFormat(
            //            tci.IsNullable
            //                ? @"(CASE WHEN (SRC.{0} IS NULL AND DST.{0} IS NULL) THEN 0 WHEN (SRC.{0} IS NULL OR DST.{0} IS NULL) THEN 1 
            //                                        WHEN ({1} <> {2}) THEN 1 ELSE 0 END) = 1"
            //                : @"{1} <> {2}",
            //            tci.NameQuoted,
            //            tci.CmpValue("SRC"),
            //            tci.CmpValue("DST"));
            //        sbMatched.AppendFormat(") {0} \n", (idx < (testCols.Count - 1)) ? "OR" : "");
            //    }
            //    sbMatched.AppendLine(") ");

            //    sbMatched.AppendLine("                                                    THEN UPDATE SET  ");
            //    for (int idx = 0; idx < testCols.Count; idx++)
            //    {
            //        TableColInfo tci = testCols[idx];
            //        if (tci.IsIdentity) continue; // skip updating an identity column
            //        sbMatched.AppendFormat("                                                      {0} = SRC.{0} ", tci.NameQuoted);
            //        sbMatched.AppendLine((idx < (testCols.Count - 1)) ? "," : "");
            //    }
            //}

            //string strMatched = sbMatched.ToString();

            //  Prepare the MERGE statement for the destination side
            //
            QryDataMerge = string.Format(@" 
        USE [{6}];
        SET NOCOUNT ON;
        BEGIN TRANSACTION;
        DECLARE @delCount INT;
        DECLARE @insCount INT;
        {5}SET IDENTITY_INSERT {0} ON;                                     
        DELETE SRC
            FROM {0} DST
                INNER JOIN {1} SRC 
                ON ({2});
        SET @delCount=@@ROWCOUNT;
        INSERT INTO {0} 
                    ({3})
                SELECT {4}
                    FROM {1} SRC;
        SET @insCount=@@ROWCOUNT;
        {5}SET IDENTITY_INSERT {0} OFF;
        --TRUNCATE TABLE {1}
        COMMIT TRANSACTION;
        SELECT @delCount AS [_DelCount_], @insCount AS [_InsCount_];",
                DstTable.FullTableName,
                DstTempFullTableName,
                string.Join(" AND ", dstKeyNames.Where(kc => (SrcTable[kc].IsComparable)).Select(kc => string.Format("(SRC.{0} = DST.{0})", DstTable[kc].NameQuoted)).ToArray()),
                string.Join(", ", CopyColNames.Select(dc => SrcTable[dc].NameQuoted).ToArray()),
                string.Join(", ", CopyColNames.Select(sd => string.Format("SRC.{0}", SrcTable[sd].NameQuoted)).ToArray()),
                (DstTable.HasIdentity) ? "" : "-- ",
                DstTable.DbName
               // ,strMatched
                );


            //MERGE INTO { 0}
            //AS DST
            //USING { 1}
            //AS SRC
            //ON({ 2})
            //WHEN NOT MATCHED BY TARGET THEN
            //INSERT({ 3})
            //VALUES({ 4})
            //{ 7};
        }

    }

}
