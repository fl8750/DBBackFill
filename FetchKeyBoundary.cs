﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DBBackfill
{
    /// <summary>
    /// FetchKeyBoundary will return source rouws whose keys are between the lower and upper limits contained in the instance.
    /// </summary>
    public class FetchKeyBoundary : FetchKeyBase
    {

        //  SQL Query to find the key column jvalues on the last row of the next fetch group (by key order)
        //
        private string strKeyLimits = @"
SELECT  {1}
    FROM (
        SELECT  ROW_NUMBER() OVER (ORDER BY LM1.[__RowNum] DESC) AS [__LastNum],
                {2}
            FROM (
                SELECT  TOP ({5}) WITH TIES 
                        ROW_NUMBER() OVER (ORDER BY {4}) AS [__RowNum],
                        {2}
                    FROM {0} {3}
                    ORDER BY {4}
                 ) LM1
         ) LM2
    WHERE (LM2.[__LastNum] = 1);
";

        //  SQL query to return the next XXXX rows 
        //
        private string strFetchRows = @"
SELECT  {1}
    FROM {0} SRC {3}
{2}
";

        private string strFetchRowsFK = @"
        {3} JOIN {0} SRC2
            ON (SRC.{1} = SRC2.{2})
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
        /// <param name="curPtNumber"></param>
        /// <param name="isFirstFetch"></param>
        /// <param name="copyColNames"></param>
        /// <param name="keyColNames"></param>
        /// <param name="curKeys"></param>
        public override void BuildFetchQuery(SqlCommand srcCmd, TableInfo srcTable, int batchSize,
                                             int curPtNumber, bool isFirstFetch, List<string> copyColNames, List<string> keyColNames, List<object> curKeys)
        {
            int skCnt = curKeys.Count;
            if (skCnt > keyColNames.Count) skCnt = keyColNames.Count; // Calc the number of beginning limits values

            int ekCnt = EndKeyList.Count;
            if (ekCnt > keyColNames.Count) ekCnt = keyColNames.Count; // Calc the number of ending limits values

            //  Build the front CTE of the SQL query that fetchs the selected row range
            //
            StringBuilder sbFetch = new StringBuilder(); // The main fetch command stringbuilder

            StringBuilder sbSelectList = new StringBuilder(); // Select column list
            StringBuilder sbKeyList = new StringBuilder(); // List of source key columns

            for (int idx = 0; idx < keyColNames.Count; idx++)
            {
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
            if ((FlgSelectByPartition) && (srcTable.PtFunc != null))
            {
                sbWhere.AppendFormat("\n                    {0} ($PARTITION.[{1}]({2}) = {3}) \n                    ",
                    (whereCnt++ == 0) ? "WHERE" : "AND",
                    srcTable.PtFunc,
                    srcTable.PtCol.NameQuoted,
                    curPtNumber);
                ++whereCnt;
            }


            //  Build the WHERE clause to mark the start of the row fetch
            //
            if (skCnt > 0)
            {
                sbWhere.AppendFormat("      {0} \n                    ", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

                BuildKeyCompare(sbWhere, srcTable, "", keyColNames, "sk", skCnt, true, isFirstFetch);
            }

            //  Construct the end key limit boolean expression
            //
            if (ekCnt > 0)
            {
                sbWhere.AppendFormat("      {0} \n                    ", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

                BuildKeyCompare(sbWhere, srcTable, "", keyColNames, "ek", ekCnt, false);

                //  Create the SqlParameters for this command
                //
                for (int idx = 0; idx < ekCnt; ++idx) // Prepare the start key value parameters
                {
                    string pName = string.Format("@ek{0}", idx + 1);
                    if (!srcCmd.Parameters.Contains(pName))
                    {
                        SqlDbType dbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                        srcCmd.Parameters.Add(pName, dbType);
                    }

                    srcCmd.Parameters[pName].Value = EndKeyList[idx];
                }

            }

            //  Check for an "AndWhere" clause
            //
            if (!string.IsNullOrEmpty(AndWhere))
            {
                sbWhere.AppendFormat("\n                    {0} ", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword
                sbWhere.AppendFormat("({0}) \n                    ", AndWhere);
            }

            // Now build the full fetch script
            //
            sbFetch.AppendFormat(strKeyLimits,
                srcTable.FullTableName,
                sbSelectList.ToString(),
                sbSelectList.ToString(),
                (whereCnt == 0) ? "" : sbWhere.ToString(),
                sbKeyList.ToString(),
                batchSize
            );
            FetchKeyLimitsSql = sbFetch.ToString(); // Save th key limit query SQL


            // ===========================================================================
            //
            //  Now build the full rows fetch
            //
            sbFetch = new StringBuilder(); // Reset the string builder
            sbFetch.AppendFormat(strFetchRows,
                srcTable.FullTableName,
                string.Join(
                    ", \n        ",
                    FKeyCopyCols.Select(col => string.Concat(col.LoadExpression.Replace("[__X__]", String.Concat("SRC.", (object)col.NameQuoted)),
                                            " AS ",
                                            col.NameQuoted))
                                .ToArray()),
                (RLConfigured != RLType.NoRelationSet)
                    ? string.Format(strFetchRowsFK,
                        RLTable.FullTableName,
                        RLSrcTableCol.NameQuoted,
                        RLTableCol.NameQuoted,
                        (RLConfigured == RLType.InnerJoin) ? "INNER" : "LEFT OUTER")
                    : "",
                (String.IsNullOrEmpty(TableHint)) ? "" : "WITH (" + TableHint + ")"
            );



            //  Construct the end key limit boolean expression for the main fetch
            //
            whereCnt = 0;

            //  If required, fetch rows from each partition individually (performance)
            //
            if (FlgSelectByPartition && (srcTable.PtFunc != null))
            {
                sbFetch.AppendFormat("       WHERE ($PARTITION.[{0}]({1}) = {2}) \n", srcTable.PtFunc, srcTable.PtCol.NameQuoted, curPtNumber);
                ++whereCnt;
            }

            //  Build the WHERE clause to mark the start of the row fetch
            //
            if (skCnt > 0)
            {
                sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

                BuildKeyCompare(sbFetch, srcTable, "SRC", keyColNames, "sk", skCnt, true, isFirstFetch);


                //  Create the SqlParameters needed for this command
                //
                for (int idx = 0; idx < skCnt; ++idx) // Prepare the start key value parameters
                {
                    string pName = string.Format("@sk{0}", idx + 1);
                    if (!srcCmd.Parameters.Contains(pName))
                    {
                        SqlDbType dbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                        srcCmd.Parameters.Add(pName, dbType);
                    }

                    srcCmd.Parameters[pName].Value = curKeys[idx];
                }

            }

            //  Construct the end key limit boolean expression
            //
            sbFetch.AppendFormat("      {0} \n", (whereCnt++ == 0) ? "WHERE" : "AND"); // Add the proper keyword

            BuildKeyCompare(sbFetch, srcTable, "SRC", keyColNames, "lk", keyColNames.Count, false);

            //  Check for an "AndWhere" clause
            //
            if (!string.IsNullOrEmpty(AndWhere))
            {
                sbFetch.AppendFormat("\n      {0} ({1})  \n", 
                    (whereCnt++ == 0) ? "WHERE" : "AND"
                    , AndWhere); // Add the proper keyword
            }

            FetchBatchSql = sbFetch.ToString(); // Save the row fetch query

        }


        //  Output a WHERE Clause expression
        //
        public StringBuilder BuildSelectFromSrc()
        {
            return null;
        }


        //  Build the logical expression use in the WHERE clause when comparing the key fields of rin/out of range
        //
        private void BuildKeyCompare(StringBuilder sbOut, TableInfo srcTable, string srcTableAlias,
                                        List<string> keyColNames, string keyPrefix, int keyValueCount,
                                        bool isLowerLimit, bool isFirstFetch = false)
        {
            // Construct the logical clauses to mark the beginning of the fetch range
            sbOut.Append("        (");
            for (int oidx = keyValueCount; oidx > 0; oidx--)
            {
                if (keyValueCount > 1) sbOut.Append("(");
                for (int iidx = 0; iidx < oidx; ++iidx)
                {
                    sbOut.AppendFormat("({0}{1} {2} @{4}{3})",
                        string.IsNullOrEmpty(srcTableAlias)
                            ? ""
                            : string.Concat(srcTableAlias, "."),
                        srcTable[keyColNames[iidx]].NameQuoted,
                        ((oidx - iidx) > 1)
                            ? "="
                            : (isLowerLimit)
                                ? ((iidx + 1) == keyValueCount)
                                    ? ((isFirstFetch) ? ">=" : ">")
                                    : ">"
                                : ((iidx + 1) == keyValueCount)
                                    ? "<="
                                    : "<",
                        (iidx + 1),
                        keyPrefix);
                    if ((oidx - iidx) > 1) sbOut.Append(" AND ");
                }

                if (keyValueCount > 1) sbOut.Append(")");

                if (oidx > 1)
                {
                    sbOut.AppendLine(" OR ");
                    sbOut.Append("        ");
                }
            }

            sbOut.AppendLine(") \n");
        }



        //  Constructors
        //
        public FetchKeyBoundary(TableInfo srcTable, string keyColNames)
            : base(srcTable, keyColNames)
        {
        }

        public FetchKeyBoundary(TableInfo srcTable, List<string> keyColNames)
            : base(srcTable, keyColNames)
        {
        }
    }

}