using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace DBBackfill
{

    public partial class BackfillContext
    {

        //  Worker properties
        //
        public void BackfillData(FetchKeyBoundary fkb,
                                 int batchSize,
                                 int connectTimeout, 
                                 List<string> srcKeyNames,
                                 List<string> dstKeyNames = null
                                 )
        {
            //  Local information
            //
            bool isSameInstance = false;
            bool hasRestarted = false; // 
            int fetchCountSinceStart = 0; // Count the number of row fetchs since start

            int curFetchCount = 0; // Number of rows fetched from the source table
            int curMergeCount = 0; // Number of rows merged into the destination table


            //  Prepare the SQL connections to the source and destination databases
            //
            SqlConnection srcConn = BackfillCtl.OpenDB(SrcTable.InstanceName, connectTimeout, SrcTable.DbName);
            SqlConnection dstConn = BackfillCtl.OpenDB(DstTable.InstanceName, connectTimeout, DstTable.DbName);

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
                BkfCtrl.DebugOutput(string.Format("Backfill Start v{0}",
                        this.BkfCtrl.Version
                       ));

                BkfCtrl.DebugOutput(string.Format("Source table: [{0}].[{1}].{2}",
                        SrcTable.InstanceName,
                        SrcTable.DbName,
                        SrcTable.FullTableName
                       ));

                BkfCtrl.DebugOutput(string.Format("Destination table: [{0}].[{1}].{2}",
                        DstTable.InstanceName,
                        DstTable.DbName,
                        DstTable.FullTableName
                       ));

                //  Show the rowcount
                BkfCtrl.DebugOutput(string.Format(" Fill Type: {0}",
                    FillType.ToString()
                ));

                //  Show the rowcount
                BkfCtrl.DebugOutput(string.Format("      Rows: {0:#,##0}  BatchChunkSize: {1:#,##0}  Partitions(#/>0): {2:#,##0}/{3:#,##0}",
                    SrcTable.RowCount,
                    batchSize,
                    SrcTable.PtCount, SrcTable.PtNotEmpty.Count
                ));

                if (FillType != BackfillType.BulkInsert)
                    PrepareWorker(srcConn, dstConn, dstKeyNames); // Setup the needed SQL environment

                //  Reset data pump loop counters
                //
                FetchRowCount = 0; // Total number of rows feched
                MergeRowCount = 0; // Total number of rows inserted into the dest table

                int ptIdx = 0;
                if ( fkb.FlgRestart && (fkb.RestartPartition != 1))
                {
                    BkfCtrl.DebugOutput(string.Format("      Restart partition: {0}", fkb.RestartPartition));
                    for (ptIdx = 0; ptIdx < SrcTable.PtNotEmpty.Count; ptIdx++)
                        if (SrcTable.PtNotEmpty[ptIdx].PartitionNumber >= fkb.RestartPartition)
                            break; // If the selected partition does exists then start at the next highest or the last
                }

                //  Partition Loop -- Once per partition
                //
                for (; ptIdx < ((fkb.FlgSelectByPartition) ? SrcTable.PtNotEmpty.Count : 1); ptIdx++)
                {
                    int curPartition = SrcTable.PtNotEmpty[ptIdx].PartitionNumber;    // Current partition number
                    hasRestarted = true;  // Assume a restart at the start of each partition

                    //  
                    //  Setup the initial key value list
                    //
                    List<object> currentFKeyList = new List<object>();
                    if (hasRestarted)
                    {
                        if ((fetchCountSinceStart == 0) && fkb.FlgRestart)
                        {
                            for (int idx = 0; (idx < fkb.RestartKeyList.Count) && (idx < srcKeyNames.Count); idx++)
                            {
                                currentFKeyList.Add(fkb.RestartKeyList[idx]); // Process any restart keys values
                            }
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
                        string sqlFetchData = "";
                        List<object> nextFKeyList;

                        SqlTransaction trnMerge;

                        //   Fetch the next set of rows into a DataTable
                        //
                        using (DataTable srcDt = new DataTable())
                        {
                            using (SqlCommand cmdSrcDb = new SqlCommand("", srcConn)) // Source database command
                            {
                                fkb.BuildFetchQuery(cmdSrcDb, 
                                    SrcTable, 
                                    batchSize, 
                                    curPartition, 
                                    (FetchLoopCount == 0) || (currentFKeyList.Count > 0), 
                                    CopyColNames, 
                                    srcKeyNames, 
                                    currentFKeyList);

                                //  First, fetch the end key limits
                                //
                                cmdSrcDb.CommandType = CommandType.Text;
                                cmdSrcDb.CommandText = fkb.FetchKeyLimitsSql;
                                cmdSrcDb.CommandTimeout = BkfCtrl.CommandTimeout;

                                using (SqlDataReader srcRdr = cmdSrcDb.ExecuteReader()) // Fetch data into a data reader
                                {
                                    srcDt.Load(srcRdr); // Load the data into a DataTable
                                    srcRdr.Close(); // Close the reader
                                }

                                //  If there are any rows in the range, continue to the fetch 
                                //
                                if (srcDt.Rows.Count > 0)
                                {
                                    for (int idx = 0; idx < srcKeyNames.Count; idx++)
                                    {
                                        string pName = string.Format("@lk{0}", idx + 1);
                                        if (!cmdSrcDb.Parameters.Contains(pName))
                                        {
                                            SqlDbType dbType = (SqlDbType) Enum.Parse(typeof(SqlDbType), SrcTable[srcKeyNames[idx]].Datatype, true);
                                            cmdSrcDb.Parameters.Add(pName, dbType);
                                        }

                                        cmdSrcDb.Parameters[pName].Value = srcDt.Rows[0][srcKeyNames[idx]];
                                    }

                                    nextFKeyList = fkb.FetchNextKeyList(srcDt.Rows[0]); // Get the last row key
                                    srcDt.Clear(); // Clear out the current contents of the DataTable

                                    //  Now fetch the next batch of rows
                                    //
                                    cmdSrcDb.CommandText = fkb.FetchBatchSql;
                                    cmdSrcDb.CommandType = CommandType.Text;
                                    cmdSrcDb.CommandTimeout = BkfCtrl.CommandTimeout;

                                    using (SqlDataReader srcRdr = cmdSrcDb.ExecuteReader()) // Fetch data into a data reader
                                    {
                                        srcDt.Load(srcRdr); // Load the data into a DataTable
                                        srcRdr.Close(); // Close the reader
                                    }                                  
                                }
                                else
                                {
                                    nextFKeyList = new List<object>();  // Nothing scanned
                                }

                                //  Get the key values from the last row in the batch
                                //
                                curFetchCount = srcDt.Rows.Count;
                                if (curFetchCount <= 0) break; // Nothing fetched, exit the data pump loop
                                FetchRowCount += curFetchCount;

                                ++FetchLoopCount; // Increment the loop count
                                ++fetchCountSinceStart; // Increment total fetch count
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
                                    if ((FillType == BackfillType.BulkInsert) /* || (FillType == BackfillType.BulkInsertMerge) */)
                                    {
                                        BulkInsertIntoTable(srcDt, trnMerge, dstConn, DstTable.FullTableName, fkb.FKeyCopyCols, BkfCtrl.CommandTimeout);
                                        //BulkInsertIntoTable(srcDt, trnMerge, dstConn, DstTable.FullTableName, CopyColNames);
                                        curMergeCount = srcDt.Rows.Count;
                                    }

                                    if ((FillType == BackfillType.Merge) /* || (FillType == BackfillType.BulkInsertMerge) */)
                                    {
                                        BulkInsertIntoTable(srcDt, trnMerge, dstConn, DstTempFullTableName, fkb.FKeyCopyCols, BkfCtrl.CommandTimeout);
                                        using (SqlCommand cmdDstMerge = new SqlCommand(QryDataMerge, dstConn, trnMerge)) //  Destination datbase connection
                                        {
                                            cmdDstMerge.CommandTimeout = BkfCtrl.CommandTimeout; // Set the set timeout value
                                            using (DataTable dtMerge = new DataTable())
                                            {
                                                using (SqlDataReader mrgRdr = cmdDstMerge.ExecuteReader()) // Merge the bulk insert rows and return stats
                                                {
                                                    dtMerge.Load(mrgRdr); // Load the data into a DataTable
                                                    mrgRdr.Close(); // Close the reader)
                                                }

                                                curMergeCount = (int) dtMerge.Rows[0]["_InsCount_"];
                                                int curDelCount = (int) dtMerge.Rows[0]["_DelCount_"];
                                            }
                                        }
                                    }

                                    MergeRowCount += curMergeCount;

                                    //  Now commit the bulk insert.  This sometimes hits a timeout which nobody can explain.  So we will just capture it
                                    //
                                    try
                                    {
                                        trnMerge.Commit();  // Good insert - Commit it
                                    }
                                    catch (Exception commitEx)
                                    {
                                        BkfCtrl.DebugOutputException(commitEx);  // Error on commit --  Make note and continue
                                    }
                                }
                                catch (Exception insertEx)
                                {
                                    BkfCtrl.DebugOutputException(insertEx);
                                    try
                                    {
                                        trnMerge.Rollback();  // We had an error
                                    }
                                    catch (Exception rollbackEx)
                                    {
                                        BkfCtrl.DebugOutputException(rollbackEx);
                                    }

                                    throw insertEx; // Throw the original exception
                                }
                                finally
                                {
                                    trnMerge.Dispose(); // Deallocate transaction object
                                    trnMerge = null;
                                }
                            }
                            srcDt.Clear();  // Deallocate the DataTable now
                        }

                        //
                        //  Output the restart information
                        //
                        if (BkfCtrl.Debug > 0)
                        {
                            for (int rKeyNo=0; rKeyNo < nextFKeyList.Count; rKeyNo++)
                            {
                                object rKey = nextFKeyList[rKeyNo];  // Get the next restart key
                                string rKeyType = rKey.GetType().Name;  // Get the .NET datatype
                                string rKeyValue;
                                switch (rKey.GetType().Name)
                                {
                                    case "string":
                                        rKeyValue = $"\"{rKey}\"";
                                        break;

                                    case "DateTime":
                                        rKeyValue = string.Format("\"{0}\"",((DateTime)rKey).ToString("yyyy-MM-dd HH:mm:ss.fff"));
                                        break;

                                    default:
                                        rKeyValue = rKey.ToString();
                                        break;
                                }

                                //  Output the current partition number
                                //
                                if (rKeyNo == 0)
                                    BkfCtrl.DebugOutput(string.Format("$RestartPartition = {0}  # Partition No;",
                                        curPartition
                                        ));

                                BkfCtrl.DebugOutput(string.Format("$RestartKeys += [{0}] {1} # Restart key[{2}] - [{3}];",
                                    rKeyType,
                                    rKeyValue,
                                    rKeyNo,
                                    CopyColNames[rKeyNo]
                                    ));
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
                                ((nextFKeyList != null) && (nextFKeyList.Count > 0)) ? nextFKeyList[0].ToString() : "---",
                                curPartition,
                                FetchLoopCount
                                ));
                        }

                        currentFKeyList = nextFKeyList; // Set the current key list with the key values from last fetched row
                    }
                }

                //  Remove the temp table after we are finished with this table
                //
                if ((FillType == BackfillType.Merge) /*|| (FillType == BackfillType.BulkInsertMerge) */ )
                {
                    using (SqlCommand cmdDstTruc = new SqlCommand(string.Format("DROP TABLE {0} ", DstTempFullTableName), dstConn)) //  Destination datbase connection
                    {
                        cmdDstTruc.ExecuteNonQuery();
                    }
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
                Exception ex2 = ex;

                if (ex2.GetType().Name != typeof(ApplicationException).Name)
                {
                    BkfCtrl.CapturedException = ex; // Save the exception information 
                    int exNest = 0;
                    while (ex2 != null)
                    {
                        BkfCtrl.DebugOutput(string.Format("Exception: [{0}] {1}", exNest, ex2.Message));
                        BkfCtrl.DebugOutput(string.Format("Exception: [{0}] {1}", exNest, ex2.StackTrace));
                        ex2 = ex2.InnerException;
                        ++exNest;
                    }
                    throw new ApplicationException("Worker Error: ", ex);
                }
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
        private int BulkInsertIntoTable(DataTable srcDt, 
                                        SqlTransaction trnActive, 
                                        SqlConnection dstConn, 
                                        string destTableName, 
                                        List<TableColInfo> copyCols,
                                        int cmdTimeout)
        {
            SqlBulkCopyOptions bcpyOpts = SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers ;
            if (DstTable.HasIdentity) bcpyOpts |= (SqlBulkCopyOptions.KeepIdentity);  // Enable IDENTITY INSERT if the destination table has an IDENTITY column

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(dstConn, bcpyOpts, trnActive))
            {
                try
                {
                    bulkCopy.BulkCopyTimeout = cmdTimeout;
                    bulkCopy.DestinationTableName = destTableName;

                    // Setup explicit column mapping to avoid any problem from the default mapping behavior
                    //
                    foreach (TableColInfo ccn in copyCols)
                    {
                        TableColInfo tci = DstTable[ccn.Name];
                        if (tci.IsIncluded)
                        {
                            SqlBulkCopyColumnMapping newCMap = new SqlBulkCopyColumnMapping(tci.Name, tci.Name);
                            bulkCopy.ColumnMappings.Add(newCMap);
                        }
                    }

                    bulkCopy.WriteToServer(srcDt); // Write from the source to the destination.
                    return srcDt.Rows.Count;
                }
                catch (Exception bulkCopyEx)
                {
                    //BkfCtrl.DebugOutput(string.Format("Exception: {0}", bulkCopyEx.Message));
                    //BkfCtrl.DebugOutput(string.Format("Exception: {0}", bulkCopyEx.StackTrace));
                    throw new ApplicationException("BulkInsertIntoTable: ", bulkCopyEx);
                }
            }
        }


        // ===============================================================================================================================
        //
        //  Prepare the destination database, work table and query texts for the backfill operation
        //
        public void PrepareWorker(SqlConnection srcConn, SqlConnection dstConn, List<string> dstKeyNames)
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
                        sqlTableDrop = string.Format(@"IF OBJECT_ID('tempdb..[{0}]') IS NOT NULL DROP TABLE {1}; ", DstTempTableName, DstTempFullTableName);
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
                                        FROM [{0}].{1}
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


            //  Prepare the MERGE statement for the destination side
            //
            QryDataMerge = string.Format(@" 
        USE [{0}];
        SET NOCOUNT ON;
        BEGIN TRANSACTION;
        DECLARE @delCount INT;
        DECLARE @insCount INT;
        {3}SET IDENTITY_INSERT {2} ON;                                     
        DELETE {7} -- DST
            FROM {2} DST
                INNER JOIN {1} SRC 
                ON ({4});
        SET @delCount=@@ROWCOUNT;
        INSERT INTO {2} 
                    ({5})
                SELECT {6}
                    FROM {1} SRC;
        SET @insCount=@@ROWCOUNT;
        {3}SET IDENTITY_INSERT {2} OFF;
        TRUNCATE TABLE {1}
        COMMIT TRANSACTION;
        SELECT @delCount AS [_DelCount_], @insCount AS [_InsCount_];",
                DstTable.DbName,
                DstTempFullTableName,
                DstTable.FullTableName,
                (DstTable.HasIdentity) ? "" : "-- ",
                string.Join(" AND ", dstKeyNames.Where(kc => (SrcTable[kc].IsComparable)).Select(kc => string.Format("(SRC.{0} = DST.{0})", DstTable[kc].NameQuoted)).ToArray()),
                string.Join(", ", CopyColNames.Select(dc => SrcTable[dc].NameQuoted).ToArray()),
                string.Join(", ", CopyColNames.Select(sd => string.Format("SRC.{0}", SrcTable[sd].NameQuoted)).ToArray()),
                (FillType == BackfillType.GapFill) ? "SRC" : "DST"
                );

        }

    }

}
