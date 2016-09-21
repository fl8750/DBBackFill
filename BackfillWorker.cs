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
        //  Worker properties
        //
        public void BackfillData(FetchKeyBoundary fkb,
                                 int batchSize,
                                 List<string> srcKeyNames,
                                 List<string> dstKeyNames = null
                                 )
        {
            //  Prepare the list of fetch key columns and merge key columns
            //
            if ((srcKeyNames == null) || (srcKeyNames.Count < 1))
            {
                srcKeyNames = fkb.FKeyColNames;
                //throw new ApplicationException("No fetch key columns names specified");
            }

            if ((dstKeyNames == null) || (dstKeyNames.Count < 1))
            {
                dstKeyNames = DstKeyNames;
            }

            //  Prepare the SQL connections to the source and destination databases
            //
            SqlConnection srcConn = BackfillCtl.OpenDB(SrcTable.InstanceName, SrcTable.DbName);
            SqlConnection dstConn = BackfillCtl.OpenDB(DstTable.InstanceName, DstTable.DbName);

            try
            {
                BkfCtrl.DebugOutput(string.Format("Backfill Start [{0}].[{1}].{2} --> [{3}].[{4}].{5}\n      Rows: {7:#,##0}  BatchChunkSize: {6:#,##0}",
                        SrcTable.InstanceName,
                        SrcTable.DbName,
                        SrcTable.FullTableName,
                        DstTable.InstanceName,
                        DstTable.DbName,
                        DstTable.FullTableName,
                        batchSize, 
                        DstTable.RowCount
                       ));

                PrepareWorker(dstConn, dstKeyNames); // Setup the needed SQL environment

                //  Reset data pump loop counters
                //
                FetchLoopCount = 0; // Number of completed fetchs
                FetchRowCount = 0; // Total number of rows feched
                MergeRowCount = 0; // Total number of rows inserted into the dest table

                //  
                //  Setup the initial key value list
                //
                List<object> currentFKeyList = new List<object>();
                for (int idx = 0; (idx < fkb.StartKeyList.Count) && (idx < srcKeyNames.Count); idx++)
                {
                    currentFKeyList.Add(fkb.StartKeyList[idx]); // Copy a key value
                }

                //
                //  Main Data Pump loop
                //
                while (true)
                {
                    DataTable srcDt;
                    int curFetchCount;
                    int curMergeCount;
                    List<object> nextFKeyList;
                    using (SqlCommand cmdSrcDb = new SqlCommand("", srcConn)) // Source database command
                    {
                        //  Fetch the next batch of data
                        //
                        string strFetchSql = fkb.GetFetchQuery(SrcTable, cmdSrcDb, (FetchLoopCount == 0), srcKeyNames, currentFKeyList);
                        string sqlFetchData = string.Format(
                            strFetchSql,
                            batchSize,
                            string.Join(", ", CopyColNames.Select(ccn => string.Format("SRC.[{0}]", ccn)).ToArray())
                            ); // Complete the fetch query setup
                        cmdSrcDb.CommandText = sqlFetchData;
                        cmdSrcDb.CommandType = CommandType.Text;

                        //  Fetch the next group of data rows from the source table
                        //
                        cmdSrcDb.CommandTimeout = 600;
                        using (SqlDataReader srcRdr = cmdSrcDb.ExecuteReader()) // Fetch data into a data reader
                        {
                            srcDt = new DataTable();
                            srcDt.Load(srcRdr); // Load the data into a DataTable
                            srcRdr.Close(); // Close the reader
                        }

                        curFetchCount = srcDt.Rows.Count;
                        FetchRowCount += curFetchCount;
                        if (curFetchCount <= 0) break; // Nothing fetched, exit the data pump loop
                        ++FetchLoopCount;

                        nextFKeyList = fkb.FetchNextKeyList(srcDt.Rows[curFetchCount - 1]);
                    }

                    //  Create a transaction object  - Protect the bulk import, merge and truncate statement in a transaction
                    //
                    SqlTransaction trnMerge = dstConn.BeginTransaction();

                    //  Setup for the SqlBulk Insert to the destination temp table
                    //
                    SqlBulkCopyOptions bcpyOpts = (DstTable.HasIdentity)
                        ? (SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls)
                        : SqlBulkCopyOptions.KeepNulls;
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(dstConn, bcpyOpts, trnMerge))
                    {
                        try
                        {
                            bulkCopy.BulkCopyTimeout = 600;
                            bulkCopy.DestinationTableName = DstTempFullTableName;

                            // Setup explicit column mapping to avoid any problem from the default mapping behavior
                            //
                            foreach (string ccn in CopyColNames)
                            {
                                TableColInfo tci = DstTable[ccn];
                                if (tci.IsIncluded)
                                {
                                    SqlBulkCopyColumnMapping newCMap = new SqlBulkCopyColumnMapping(tci.Name, tci.Name);
                                    bulkCopy.ColumnMappings.Add(newCMap);
                                }
                            }
                            bulkCopy.WriteToServer(srcDt); // Write from the source to the destination.
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            throw;
                        }
                        finally
                        {
                            srcDt.Clear();
                        }
                    }

                    //  Now merge the temp table into the final destination table
                    //
                    using (SqlCommand cmdDstMerge = new SqlCommand(QryDataMerge, dstConn, trnMerge)) //  Destination datbase connection
                    {
                        cmdDstMerge.CommandTimeout = 600;
                        curMergeCount = (int) cmdDstMerge.ExecuteScalar();
                        MergeRowCount += curMergeCount;
                    }

                    //  Last step -- write a progress record for restart purposes
                    //
                    if (BkfCtrl.Debug > 0)
                    {
                        BkfCtrl.DebugOutput(string.Format("-- [{7}] F:{0:#,##0}/{1:#,##0}/{2:#,##0} M:{3:#,##0}/{4:#,##0} {5}/{6}",
                            curFetchCount,
                            FetchRowCount,
                            DstTable.RowCount,
                            curMergeCount,
                            MergeRowCount,
                            ((currentFKeyList != null) && (currentFKeyList.Count > 0)) ? currentFKeyList[0].ToString() : "---",
                            ((fkb.EndKeyList != null) && (fkb.EndKeyList.Count > 0)) ? fkb.EndKeyList[0].ToString() : "---",
                            FetchLoopCount
                            ));
                    }

                    trnMerge.Commit();
                    currentFKeyList = nextFKeyList; // Set the current key list with the key values from last fetched row
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

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                BackfillCtl.CloseDb(srcConn);
                BackfillCtl.CloseDb(dstConn);
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
                dstConn.ChangeDatabase("tempdb");       // Switch to the temp datababse

                //  Create the destination work table if not there, or drop any existing table.
                //
                try
                {
                    string sqlTableDrop = string.Format(@"IF OBJECT_ID('tempdb..{0}') IS NOT NULL DROP TABLE {0}; ",
                        DstTempFullTableName,
                        DstTempTableName);
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

                string sqlCreateWTab = string.Format(strCreateWTab,
                    DstTable.DbName,
                    DstTable.FullTableName,
                    DstTempFullTableName,
                    TempSchemaName,
                    DstTempTableName,
                    string.Join(", ", dstKeyNames.Select(cn => string.Format("[{0}]", cn)).ToArray()));

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
            StringBuilder sbMatched = new StringBuilder();

            List<TableColInfo> testCols = DstTable.Where(tci => (CopyColNames.Contains(tci.Name))).Where(tci => (!dstKeyNames.Contains(tci.Name))).ToList();

            if (testCols.Count > 0)
            {
                sbMatched.AppendLine("WHEN MATCHED AND (");
                for (int idx = 0; idx < testCols.Count; idx++)
                {
                    TableColInfo tci = testCols[idx];
                    if ((!tci.IsComparable) || (tci.IsIdentity)) continue;
                    sbMatched.Append("                                                  (");
                    sbMatched.AppendFormat(
                        tci.IsNullable
                            ? @"(CASE WHEN (SRC.{0} IS NULL AND DST.{0} IS NULL) THEN 0 WHEN (SRC.{0} IS NULL OR DST.{0} IS NULL) THEN 1 
                                                    WHEN ({1} <> {2}) THEN 1 ELSE 0 END) = 1"
                            : @"{1} <> {2}",
                        tci.NameQuoted,
                        tci.CmpValue("SRC"),
                        tci.CmpValue("DST"));
                    sbMatched.AppendFormat(") {0} \n", (idx < (testCols.Count - 1)) ? "OR" : "");
                }
                sbMatched.AppendLine(") ");

                sbMatched.AppendLine("                                                    THEN UPDATE SET  ");
                for (int idx = 0; idx < testCols.Count; idx++)
                {
                    TableColInfo tci = testCols[idx];
                    if (tci.IsIdentity) continue; // skip updating an identity column
                    sbMatched.AppendFormat("                                                      {0} = SRC.{0} ", tci.NameQuoted);
                    sbMatched.AppendLine((idx < (testCols.Count - 1)) ? "," : "");
                }
            }

            string strMatched = sbMatched.ToString();

            //  Prepare the MERGE statement for the destination side
            //
            QryDataMerge = string.Format(@" USE [{6}];
                                            SET NOCOUNT ON;
                                            DECLARE @mCount INT;
                                            {5}SET IDENTITY_INSERT {0} ON;
                                            MERGE INTO {0} AS DST 
                                                  USING {1} AS SRC
                                                     ON ({2})
                                                  WHEN NOT MATCHED BY TARGET THEN 
                                                     INSERT ({3})
                                                     VALUES ({4})
                                                  {7};
                                            SET @mCount=@@ROWCOUNT;
                                            {5}SET IDENTITY_INSERT {0} OFF;
                                            TRUNCATE TABLE {1};
                                            SELECT @mCount;",
                DstTable.FullTableName,
                DstTempFullTableName,
                string.Join(" AND ", DstKeyNames.Where(kc => (SrcTable[kc].IsComparable)).Select(kc => string.Format("(SRC.{0} = DST.{0})", DstTable[kc].NameQuoted)).ToArray()),
                string.Join(", ", CopyColNames.Select(dc => SrcTable[dc].NameQuoted).ToArray()),
                string.Join(", ", CopyColNames.Select(sd => string.Format("SRC.{0}", SrcTable[sd].NameQuoted)).ToArray()),
                (DstTable.HasIdentity) ? "" : "-- ",
                DstTable.DbName,
                strMatched
                );
        }

    }

}
