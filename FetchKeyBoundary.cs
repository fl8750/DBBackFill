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

        private string strKeyLimits = @"
SELECT  {1}
    FROM (
        SELECT  ROW_NUMBER() OVER (ORDER BY LM1.[__RowNum] DESC) AS [__LastNum],
                {2}
            FROM (
                SELECT  TOP ({5}) WITH TIES 
                        ROW_NUMBER() OVER (ORDER BY {4}) AS [__RowNum],
                        {2}
                    FROM {0}
					{3}
                    ORDER BY {4}
                    ) LM1
            ) LM2
    WHERE (LM2.[__LastNum] = 1);
";

        private string strFetchRows = @"
SELECT  {1}
    FROM {0}
";


        public override List<object> FetchNextKeyList(DataRow lastDataRow)
        {
            return FKeyColNames.Select(kcName => lastDataRow[kcName]).ToList();
        }

        /// <summary>
        /// Build the SQL script used to fetch the next batch of rows from the source table
        /// </summary>
        /// <param name="srcCmd"></param>
        /// <param name="srcTable"></param>
        /// <param name="batchSize"></param>
        /// <param name="partNumber"></param>
        /// <param name="isFirstFetch"></param>
        /// <param name="copyColNames"></param>
        /// <param name="keyColNames"></param>
        /// <param name="curKeys"></param>
        public override void BuildFetchQuery(SqlCommand srcCmd, TableInfo srcTable, int batchSize,
                                             int partNumber, bool isFirstFetch, List<string> copyColNames, List<string> keyColNames, List<object> curKeys)
        {
            int skCnt = curKeys.Count;
            if (skCnt > keyColNames.Count) skCnt = keyColNames.Count; // Calc the number of beginning limits values

            int ekCnt = EndKeyList.Count;
            if (ekCnt > keyColNames.Count) ekCnt = keyColNames.Count; // Calc the number of ending limits values

            //  Build the front CTE of the SQL query that fetchs the selected row range
            //
            StringBuilder sbFetch = new StringBuilder(); // The main fetch command stringbuilder

            StringBuilder sbDeclare = new StringBuilder(); // The declaration of the end limit capture variables
            StringBuilder sbCapture = new StringBuilder(); // The capture of the end limits
            StringBuilder sbSelectList = new StringBuilder(); // Select column list
            StringBuilder sbKeyList = new StringBuilder(); // List of source columns

            for (int idx = 0; idx < keyColNames.Count; idx++)
            {
                //  Add the declaration of the variables used to hold the last keys fetche in the batch
                //
                sbDeclare.AppendFormat("DECLARE   @lk{0}   {1}; -- {2}\n",
                    idx + 1,
                    srcTable[keyColNames[idx]].DatatypeFull,
                    srcTable[keyColNames[idx]].NameQuoted);

                //  Now capture the last fetched values
                //
                sbCapture.AppendFormat("      @lk{0} = {1}{2}\n",
                    idx + 1,
                    srcTable[keyColNames[idx]].NameQuoted,
                    (idx == (keyColNames.Count - 1)) ? "" : ",");

                //  Select the key columns from the source table
                //
                sbSelectList.AppendFormat("                            {0}{1}\n",
                    srcTable[keyColNames[idx]].NameQuoted,
                    (idx == (keyColNames.Count - 1)) ? "" : ",");

                //  Build the ORDER BY column list
                //
                sbKeyList.AppendFormat("{0}{1} ",
                    srcTable[keyColNames[idx]].NameQuoted,
                    (idx == (keyColNames.Count - 1)) ? "" : ",");
            }


            //  Build the WHERE clause
            //
            int whereCnt = 0; // Number of generated WHERE clauses
            StringBuilder sbWhere = new StringBuilder();

            //  If required, fetch rows from each partition individually (performance)
            //
            if (FlgSelectByPartition)
            {
                sbWhere.AppendFormat("       WHERE ($PARTITION.[{0}]({1}) = {2}) \n", srcTable.PtFunc, srcTable.PtCol.NameQuoted, partNumber);
                ++whereCnt;
            }


            //  Build the WHERE clause to mark the start of the row fetch
            //
            if (skCnt > 0)
            {
                sbWhere.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

                sbWhere.Append("        ("); // Construct the logical clauses to mark the beginning of the fetch range
                for (int oidx = skCnt; oidx > 0; oidx--)
                {
                    sbWhere.Append("(");
                    for (int iidx = 0; iidx < oidx; ++iidx)
                    {
                        sbWhere.AppendFormat("({0} {1} @sk{2})",
                            srcTable[keyColNames[iidx]].NameQuoted,
                            ((oidx - iidx) > 1)
                                ? "="
                                : ((iidx + 1) == skCnt)
                                    ? ((isFirstFetch) ? ">=" : ">")
                                    : ">",
                            (iidx + 1));
                        if ((oidx - iidx) > 1) sbWhere.Append(" AND ");
                    }

                    sbWhere.Append(")");
                    if (oidx > 1) sbWhere.AppendLine(" OR ");
                }

                sbWhere.AppendLine(") \n");
            }

            //  Construct the end key limit boolean expression
            //
            if (ekCnt > 0)
            {
                sbWhere.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

                sbWhere.Append("        (");
                for (int oidx = ekCnt; oidx > 0; oidx--)
                {
                    sbWhere.Append("(");
                    for (int iidx = 0; iidx < oidx; ++iidx)
                    {
                        sbWhere.AppendFormat("({0} {1} @ek{2})",
                            srcTable[keyColNames[iidx]].NameQuoted,
                            ((oidx - iidx) > 1) ? "=" : ((iidx + 1) == ekCnt) ? "<=" : "<",
                            (iidx + 1));
                        if ((oidx - iidx) > 1) sbWhere.Append(" AND ");
                    }

                    sbWhere.Append(")");
                    if (oidx > 1) sbWhere.AppendLine(" OR ");
                }

                sbWhere.AppendLine(") \n");
            }

            // Now build the full fetch script
            //
            sbFetch.Append(sbDeclare.ToString());
            sbFetch.AppendFormat(strKeyLimits,
                srcTable.FullTableName,
                sbCapture.ToString(),
                sbSelectList.ToString(),
                sbWhere.ToString(),
                sbKeyList.ToString(),
                batchSize
            );


            // ===========================================================================
            //
            //  Now build the full rows fetch
            //
            sbFetch.AppendFormat(strFetchRows,
                srcTable.FullTableName,
                string.Join(", ", srcTable.Columns.OrderBy(col => col.Value.ID).Select(col => col.Value.NameQuoted).ToArray())
            );

            //  Construct the end key limit boolean expression for the main fetch
            //
            whereCnt = 0;

            //  If required, fetch rows from each partition individually (performance)
            //
            if (FlgSelectByPartition)
            {
                sbFetch.AppendFormat("       WHERE ($PARTITION.[{0}]({1}) = {2}) \n", srcTable.PtFunc, srcTable.PtCol.NameQuoted, partNumber);
                ++whereCnt;
            }

            //  Build the WHERE clause to mark the start of the row fetch
            //
            if (skCnt > 0)
            {
                sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

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
                                : ((iidx + 1) == skCnt)
                                    ? ((isFirstFetch) ? ">=" : ">")
                                    : ">",
                            (iidx + 1));
                        if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
                    }

                    sbFetch.Append(")");
                    if (oidx > 1) sbFetch.AppendLine(" OR ");
                }

                sbFetch.AppendLine(") \n");
            }

            //  Construct the end key limit boolean expression
            //
            //if (ekCnt > 0)
            //{
            sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

            sbFetch.Append("        (");
            for (int oidx = keyColNames.Count; oidx > 0; oidx--)
            {
                sbFetch.Append("(");
                for (int iidx = 0; iidx < oidx; ++iidx)
                {
                    sbFetch.AppendFormat("({0} {1} @lk{2})",
                        srcTable[keyColNames[iidx]].NameQuoted,
                        ((oidx - iidx) > 1) 
                            ? "=" 
                            : ((iidx + 1) == keyColNames.Count) 
                                ? "<=" 
                                : "<",
                        (iidx + 1));
                    if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
                }

                sbFetch.Append(")");
                if (oidx > 1) sbFetch.AppendLine(" OR ");
            }

            sbFetch.AppendLine(") \n");
            //}






            //sbFetch.AppendFormat("      SELECT TOP ({1}) {0} \n",
            //    (FlgOrderBy) ? "WITH TIES" : "",
            //    batchSize);
            //sbFetch.AppendFormat("        {0} \n",
            //    string.Join(", \n       ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            ////sbFetch.AppendFormat("        , ROW_NUMBER() OVER (ORDER BY {0}) AS [__ROWS__] \n",
            ////    string.Join(", ", keyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            //sbFetch.AppendFormat("          FROM {0} \n", srcTable.FullTableName);


            ////  Add the partition selection clause
            ////
            //bool flgNeedAnd = false;

            //// Build the initial start limits
            ////
            //if (skCnt > 0)
            //{
            //    sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

            //    //  Assemble start key boolean expression
            //    //
            //    if (skCnt > 0)
            //    {
            //        if (flgNeedAnd) sbFetch.AppendLine(" AND "); // Connect the expression using an AND
            //        for (int idx = 0; idx < skCnt; ++idx) // Prepare the start key value parameters
            //        {
            //            string pName = string.Format("@sk{0}", idx + 1);
            //            if (!srcCmd.Parameters.Contains(pName))
            //            {
            //                SqlDbType dbType = (SqlDbType) Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
            //                srcCmd.Parameters.Add(pName, new SqlParameter(pName, dbType));
            //            }

            //            srcCmd.Parameters[pName].Value = curKeys[idx];
            //        }

            //        sbFetch.Append("        ("); // Construct the logical clauses to mark the beginning of the fetch range
            //        for (int oidx = skCnt; oidx > 0; oidx--)
            //        {
            //            sbFetch.Append("(");
            //            for (int iidx = 0; iidx < oidx; ++iidx)
            //            {
            //                sbFetch.AppendFormat("({0} {1} @sk{2})",
            //                    srcTable[keyColNames[iidx]].NameQuoted,
            //                    ((oidx - iidx) > 1)
            //                        ? "="
            //                        : ((iidx + 1) == skCnt)
            //                            ? ((isFirstFetch) ? ">=" : ">")
            //                            : ">",
            //                    (iidx + 1));
            //                if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
            //            }

            //            sbFetch.Append(")");
            //            if (oidx > 1) sbFetch.AppendLine(" OR ");
            //        }

            //        sbFetch.AppendLine(")");
            //        flgNeedAnd = true;
            //    }
            //}

            ////  Construct the end key limit boolean expression
            ////
            //if (ekCnt > 0)
            //{
            //    sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

            //    if (flgNeedAnd) sbFetch.AppendLine(" AND "); // Construct the logical clauses to mark the end of the fetch range

            //    for (int idx = 0; idx < ekCnt; ++idx) // Prepare the start key value parameters
            //    {
            //        string pName = string.Format("@ek{0}", idx + 1);
            //        if (!srcCmd.Parameters.Contains(pName))
            //        {
            //            SqlDbType dbType = (SqlDbType) Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
            //            srcCmd.Parameters.Add(pName, new SqlParameter(pName, dbType));
            //        }

            //        srcCmd.Parameters[pName].Value = EndKeyList[idx];
            //    }

            //    sbFetch.Append("        (");
            //    for (int oidx = ekCnt; oidx > 0; oidx--)
            //    {
            //        sbFetch.Append("(");
            //        for (int iidx = 0; iidx < oidx; ++iidx)
            //        {
            //            sbFetch.AppendFormat("({0} {1} @ek{2})",
            //                srcTable[keyColNames[iidx]].NameQuoted,
            //                ((oidx - iidx) > 1) ? "=" : ((iidx + 1) == ekCnt) ? "<=" : "<",
            //                (iidx + 1));
            //            if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
            //        }

            //        sbFetch.Append(")");
            //        if (oidx > 1) sbFetch.AppendLine(" OR ");
            //    }

            //    sbFetch.AppendLine(")");
            //}


            //sbFetch.AppendFormat("      ORDER BY {0} \n", string.Join(", ", keyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            ////sbFetch.AppendFormat("      ORDER BY [__ROWS__] \n");
            //sbFetch.AppendFormat("      ) \n"); // Complete 

            //
            // ===================================================================================================
            // Fetch the next range of data rows 
            //
            StringBuilder sbFetchBatch = new StringBuilder();

            sbFetchBatch.Append(sbFetch.ToString()); // Include the previous CTE


            //sbFetchBatch.AppendFormat("  SELECT \n");
            //sbFetchBatch.AppendFormat("      {0} \n", string.Join(", ", copyColNames.Select(ccn => string.Format("[{0}]", ccn)).ToArray()));
            //sbFetchBatch.AppendFormat("      FROM SRCTAB \n");

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
            : base(srcTable, keyColNames) { }
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
