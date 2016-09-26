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

//        private string QryDataFetch = @" 
//    SELECT TOP ({{0}}) {0}
//            {{1}} 
//        FROM {1} SRC
//        {2}
//        {3}; ";

        public override List<object> FetchNextKeyList(DataRow lastDataRow)
        {
            return FKeyColNames.Select(kcName => lastDataRow[kcName]).ToList();
        }

        public override string GetFetchQuery(TableInfo srcTable, SqlCommand cmdSrcDb, bool firstFetch, List<string> keyColNames, List<object> curKeys)
        {
            StringBuilder sbFetch = new StringBuilder();

            int skCnt = curKeys.Count;
            if (skCnt > keyColNames.Count) skCnt = keyColNames.Count;

            int ekCnt = EndKeyList.Count;
            if (ekCnt > keyColNames.Count) ekCnt = keyColNames.Count;

            //  Build the front part of the SQL query
            //
            sbFetch.AppendFormat("SELECT TOP ({{0}}) {0} \n", (FlgOrderBy) ? "WITH TIES" : "");
            sbFetch.AppendFormat("      {{1}} \n");
            sbFetch.AppendFormat("  FROM {0} SRC \n", srcTable.FullTableName);

            //  Build the WHERE clause
            //
            if ((skCnt + ekCnt) > 0) 
            {
                sbFetch.AppendFormat("  WHERE \n");

                if (skCnt > 0)
                {
                    for (int idx = 0; idx < skCnt; ++idx) // Prepare the start key value parameters
                    {
                        string pName = string.Format("@sk{0}", idx + 1);
                        SqlDbType dbType = (SqlDbType) Enum.Parse(typeof (SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                        cmdSrcDb.Parameters.Add(new SqlParameter(pName, dbType));
                        cmdSrcDb.Parameters[pName].Value = curKeys[idx];
                    }

                    sbFetch.Append("    (");    // Construct the logical clauses to mark the beginning of the fetch range
                    for (int oidx = skCnt; oidx > 0; oidx--)
                    {
                        sbFetch.Append("(");
                        for (int iidx = 0; iidx < oidx; ++iidx)
                        {
                            sbFetch.AppendFormat("(SRC.{0} {1} @sk{2})",
                                srcTable[keyColNames[iidx]].NameQuoted,
                                ((oidx - iidx) > 1)
                                    ? "="
                                    : ((iidx + 1) == skCnt) ? ((firstFetch) ? ">=" : ">") : ">",
                                (iidx + 1));                     
                            if ((oidx - iidx) > 1) sbFetch.Append(" AND ");
                        }
                        sbFetch.Append(")");
                        if (oidx > 1) sbFetch.AppendLine(" OR ");
                    }
                    sbFetch.AppendLine(")");
                }

                //  Construct the end key limits
                //
                if (ekCnt > 0)
                {
                    if (skCnt > 0) sbFetch.AppendLine(" AND "); // Construct the logical clauses to mark the end of the fetch range

                    for (int idx = 0; idx < ekCnt; ++idx) // Prepare the start key value parameters
                    {
                        string pName = string.Format("@ek{0}", idx + 1);
                        SqlDbType dbType = (SqlDbType) Enum.Parse(typeof (SqlDbType), srcTable[keyColNames[idx]].Datatype, true);
                        cmdSrcDb.Parameters.Add(new SqlParameter(pName, dbType));
                        cmdSrcDb.Parameters[pName].Value = EndKeyList[idx];
                    }

                    sbFetch.Append("    (");
                    for (int oidx = ekCnt; oidx > 0; oidx--)
                    {
                        sbFetch.Append("(");
                        for (int iidx = 0; iidx < oidx; ++iidx)
                        {
                            sbFetch.AppendFormat("(SRC.{0} {1} @ek{2})",
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

            //  Build the ORDER BY clause
            //
            if (FlgOrderBy)
                sbFetch.AppendFormat("ORDER BY {0}", string.Join(", ", keyColNames.Select(kc => ("SRC." + kc)).ToArray()));

            return sbFetch.ToString();
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
            return srcTable.CreateFetchKeyComplete((keyColName == null) ? null : keyColName.Split(',').ToList());
        }

        public static FetchKeyBoundary CreateFetchKeyComplete(this TableInfo srcTable, List<string> keyColNames)
        {
            if (keyColNames == null)
            {
                keyColNames = srcTable.Where(kc => kc.KeyOrdinal > 0).OrderBy(kc => kc.KeyOrdinal).Select(kc => kc.Name).ToList();
            }
            
            //Console.WriteLine("Key Cols: {0} - '{1}'", keyColNames.Count, keyColNames[0]);
            FetchKeyBoundary newFKB = new FetchKeyBoundary(srcTable, keyColNames);
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
                        newFKB.SqlQuery = cmdKeys.CommandText;
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
