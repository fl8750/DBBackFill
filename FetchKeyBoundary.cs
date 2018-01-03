using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace DBBackfill
{
    /// <summary>
    /// FetchKeyBoundary will return source rouws whose keys are between the lower and upper limits contained in the instance.
    /// </summary>
    public class FetchKeyBoundary : FetchKeyBase
    {

        public override List<object> FetchNextKeyList(DataRow lastDataRow)
        {
            return FKeyColNames.Select(kcName => lastDataRow[kcName]).ToList();
        }

        public override void BuildFetchQuery(SqlCommand srcCmd, TableInfo srcTable, int batchSize,
                                             int partNumber, bool isFirstFetch, List<string> copyColNames, List<string> keyColNames, List<object> curKeys)
        {

            int skCnt = curKeys.Count;
            if (skCnt > keyColNames.Count) skCnt = keyColNames.Count;

            int ekCnt = EndKeyList.Count;
            if (ekCnt > keyColNames.Count) ekCnt = keyColNames.Count;

            //  Build the front CTE of the SQL query that fetchs the selected row range
            //
            StringBuilder sbFetch = new StringBuilder();

            sbFetch.AppendFormat(";WITH SRCTAB AS ( \n");
            sbFetch.AppendFormat("      SELECT TOP ({1}) {0} \n",
                (FlgOrderBy) ? "WITH TIES" : "",
                batchSize);
            sbFetch.AppendFormat("        {0} \n",
                string.Join(", \n       ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            //sbFetch.AppendFormat("        , ROW_NUMBER() OVER (ORDER BY {0}) AS [__ROWS__] \n",
            //    string.Join(", ", keyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            sbFetch.AppendFormat("          FROM {0} \n", srcTable.FullTableName);

            //  Build the WHERE clause
            //
            bool flgNeedAnd = false;
            if (((skCnt + ekCnt) > 0) || (FlgSelectByPartition))
            {
                sbFetch.AppendFormat("      WHERE \n");

                //  If required, fetch rows from each partition individually (performance)
                //
                if (FlgSelectByPartition)
                {
                    sbFetch.AppendFormat("      ($PARTITION.[{0}]({1}) = {2}) \n", srcTable.PtFunc, srcTable.PtCol.NameQuoted, partNumber);
                    flgNeedAnd = true; // Insert the partition number to be queried
                }

                //  Assemble start key boolean expression
                //
                if (skCnt > 0)
                {
                    if (flgNeedAnd) sbFetch.AppendLine(" AND "); // Connect the expression using an AND
                    for (int idx = 0; idx < skCnt; ++idx) // Prepare the start key value parameters
                    {
                        string pName = string.Format("@sk{0}", idx + 1);
                        if (!srcCmd.Parameters.Contains(pName))
                        {
                            SqlDbType dbType = (SqlDbType) Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                            srcCmd.Parameters.Add(pName, new SqlParameter(pName, dbType));
                        }
                        srcCmd.Parameters[pName].Value = curKeys[idx];
                    }

                    sbFetch.Append("        ("); // Construct the logical clauses to mark the beginning of the fetch range
                    for (int oidx = skCnt; oidx > 0; oidx--)
                    {
                        sbFetch.Append("(");
                        for (int iidx = 0; iidx < oidx; ++iidx)
                        {
                            sbFetch.AppendFormat("({0} {1} @sk{2})",
                                srcTable[keyColNames[iidx]].NameQuoted,
                                ((oidx - iidx) > 1)
                                    ? "="
                                    : ((iidx + 1) == skCnt) ? ((isFirstFetch) ? ">=" : ">") : ">",
                                (iidx + 1));
                            if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
                        }
                        sbFetch.Append(")");
                        if (oidx > 1) sbFetch.AppendLine(" OR ");
                    }
                    sbFetch.AppendLine(")");
                    flgNeedAnd = true;
                }

                //  Construct the end key limit boolean expression
                //
                if (ekCnt > 0)
                {
                    if (flgNeedAnd) sbFetch.AppendLine(" AND "); // Construct the logical clauses to mark the end of the fetch range

                    for (int idx = 0; idx < ekCnt; ++idx) // Prepare the start key value parameters
                    {
                        string pName = string.Format("@ek{0}", idx + 1);
                        if (!srcCmd.Parameters.Contains(pName))
                        {
                            SqlDbType dbType = (SqlDbType) Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                            srcCmd.Parameters.Add(pName, new SqlParameter(pName, dbType));
                        }
                        srcCmd.Parameters[pName].Value = EndKeyList[idx];
                    }

                    sbFetch.Append("        (");
                    for (int oidx = ekCnt; oidx > 0; oidx--)
                    {
                        sbFetch.Append("(");
                        for (int iidx = 0; iidx < oidx; ++iidx)
                        {
                            sbFetch.AppendFormat("({0} {1} @ek{2})",
                                srcTable[keyColNames[iidx]].NameQuoted,
                                ((oidx - iidx) > 1) ? "=" : ((iidx + 1) == ekCnt) ? "<=" : "<",
                                (iidx + 1));
                            if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
                        }
                        sbFetch.Append(")");
                        if (oidx > 1) sbFetch.AppendLine(" OR ");
                    }
                    sbFetch.AppendLine(")");
                }
            }
            sbFetch.AppendFormat("      ORDER BY {0} \n", string.Join(", ", keyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            //sbFetch.AppendFormat("      ORDER BY [__ROWS__] \n");
            sbFetch.AppendFormat("      ) \n"); // Complete 

            //
            // ===================================================================================================
            // Fetch the next range of data rows 
            //
            StringBuilder sbFetchBatch = new StringBuilder();

            sbFetchBatch.Append(sbFetch.ToString()); // Include the previous CTE
            sbFetchBatch.AppendFormat("  SELECT \n");
            sbFetchBatch.AppendFormat("      {0} \n", string.Join(", ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            sbFetchBatch.AppendFormat("      FROM SRCTAB \n");

            FetchBatchSql = sbFetchBatch.ToString();

            //
            // ===================================================================================================
            //
            StringBuilder sbFetchLast = new StringBuilder();

            sbFetchLast.Append(sbFetch.ToString()); // Include the previous CTE
            sbFetchLast.AppendFormat("INSERT INTO {{0}} \n");
            sbFetchLast.AppendFormat("  ({0}) \n", string.Join(", \n    ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            sbFetchLast.AppendFormat("  SELECT \n");
            sbFetchLast.AppendFormat("      {0} \n", string.Join(", ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            sbFetchLast.AppendFormat("      FROM SRCTAB \n");
            sbFetchLast.AppendFormat("-- \n");
            sbFetchLast.Append(sbFetch.ToString()); // Include the previous CTE
            sbFetchLast.AppendFormat("  SELECT TOP 1 [__ROWS__], \n");
            sbFetchLast.AppendFormat("      {0} \n", string.Join(", ", keyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            sbFetchLast.AppendFormat("      FROM SRCTAB \n");
            sbFetchLast.AppendFormat("      ORDER BY [__ROWS__] DESC \n");

            FetchLastSql = sbFetchLast.ToString();

            //return;
        }

        //  Constructors
        //
        public FetchKeyBoundary(TableInfo srcTable, List<string> keyColNames)
            : base(srcTable, keyColNames) {}
    }



    // ==================================================================================================
    //
    //  Helper Class -- FetchKeyBoundary
    //
    // ==================================================================================================
    public static class FetchKeyHelpers
    {
        public static FetchKeyBoundary CreateFetchKeyComplete(this TableInfo srcTable, string keyColName = null)
        {
            return CreateFetchKeyComplete(srcTable, (keyColName == null) ? null : keyColName.Split(',').ToList());
        }

        public static FetchKeyBoundary CreateFetchKeyComplete(this TableInfo srcTable, List<string> keyColNames)
        {
            if (keyColNames == null)
            {
                keyColNames = srcTable.Where(kc => kc.KeyOrdinal > 0).OrderBy(kc => kc.KeyOrdinal).Select(kc => kc.Name).ToList();
            }
            
            //Console.WriteLine("Key Cols: {0} - '{1}'", keyColNames.Count, keyColNames[0]);
            FetchKeyBoundary newFKB = new FetchKeyBoundary(srcTable, keyColNames);

            //  Check for any partitioning performance problems
            //
            if ((srcTable.IsPartitioned) && (srcTable.PtCol.ID == srcTable[keyColNames[0]].ID))
            {
                newFKB.FlgSelectByPartition = true;
            }

            return newFKB;
        }


        public static FetchKeyBoundary CreateFetchKeyBoundary(this TableInfo srcTable, string keyColName, string whereClause)
        {
            //TODO: Add multi-column key support

            FetchKeyBoundary newFKB = null;

            string qryGetKeyLimits = @"
                SELECT MIN(SRC.{0}) AS MinKey, 
                       MAX(SRC.{0}) AS MaxKey
                    FROM {1} SRC
                    {2}";

            using (SqlConnection srcConn = BackfillCtl.OpenDB(srcTable.InstanceName, srcTable.DbName))
            {
                string strKeyQuery = string.Format(qryGetKeyLimits,
                    srcTable[keyColName].NameQuoted,
                    srcTable.FullTableName,
                    (string.IsNullOrEmpty(whereClause)) ? "" : ("WHERE " + whereClause));
                using (SqlCommand cmdKeys = new SqlCommand(strKeyQuery, srcConn))
                {
                    cmdKeys.CommandTimeout = 300; // timeout in seconds
                    DataTable dtKeys = new DataTable();
                    using (SqlDataReader rdrKeys = cmdKeys.ExecuteReader())
                    {
                        dtKeys.Load(rdrKeys);
                    }

                    if (dtKeys.Rows.Count > 0)
                    {
                        newFKB = new FetchKeyBoundary(srcTable, new List<string>() { keyColName });
                        if (dtKeys.Rows[0]["MinKey"].GetType().Name != "DBNull") newFKB.StartKeyList.Add(dtKeys.Rows[0]["MinKey"]);
                        if (dtKeys.Rows[0]["MaxKey"].GetType().Name != "DBNull") newFKB.EndKeyList.Add(dtKeys.Rows[0]["MaxKey"]);
                        //newFKB.SqlQuery = cmdKeys.CommandText;
                    }
                }
                BackfillCtl.CloseDb(srcConn); // Close the DB connection
            }
            return newFKB;
        }

        /// <summary>
        /// Create a single column boundary set
        /// </summary>
        /// <param name="srcTable">Reference to TableInfo object of the source table</param>
        /// <param name="keyColName">Key column name</param>
        /// <param name="minKey">Start value</param>
        /// <param name="maxKey">Final value</param>
        /// <returns></returns>
        public static FetchKeyBoundary CreateFetchKeyBoundary(this TableInfo srcTable, string keyColName, object minKey, object maxKey)
        {
            //TODO: Add multi-column key support

            FetchKeyBoundary newFKB = null;

            newFKB = new FetchKeyBoundary(srcTable, new List<string>(){ keyColName });
            if ((minKey != null) && (minKey.GetType().Name != "DBNull")) newFKB.StartKeyList.Add(minKey);
            if ((maxKey != null) && (maxKey.GetType().Name != "DBNull")) newFKB.StartKeyList.Add(maxKey);

            return newFKB;
        }

    }

}
