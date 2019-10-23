using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBBackfill
{
    // ==================================================================================================
    //
    //  Helper Class -- FetchKeyBoundary
    //
    // ==================================================================================================
    public static class FetchKeyHelpers
    {
        public static FetchKeyBoundary CreateFetchKeyComplete(this TableInfo srcTable, string keyColName = null)
        {
            return CreateFetchKeyComplete(srcTable, keyColName?.Split(',').ToList());
        }

        public static FetchKeyBoundary CreateFetchKeyComplete(this TableInfo srcTable, List<string> keyColNames)
        {
            if (keyColNames == null)
            {
                keyColNames = srcTable.Where(kc => kc.KeyOrdinal > 0).OrderBy(kc => kc.KeyOrdinal).Select(kc => kc.Name).ToList();
            }

            FetchKeyBoundary newFKB = new FetchKeyBoundary(srcTable, keyColNames);

            //  Check for any partitioning performance problems
            //
            if ((srcTable.IsPartitioned) && (srcTable.PtCol.ID == srcTable[keyColNames[0]].ID))
            {
                newFKB.FlgSelectByPartition = true;
            }

            return newFKB;
        }
    }


    //public static FetchKeyBoundary CreateFetchKeyBoundary(this TableInfo srcTable, string keyColName, string whereClause)
    //{
    //    //TODO: Add multi-column key support

    //    FetchKeyBoundary newFKB = null;

    //    string qryGetKeyLimits = @"
    //        SELECT MIN(SRC.{0}) AS MinKey, 
    //               MAX(SRC.{0}) AS MaxKey
    //            FROM {1} SRC
    //            {2}";

    //    using (SqlConnection srcConn = BackfillCtl.OpenDB(srcTable.InstanceName, srcTable.DbName))
    //    {
    //        string strKeyQuery = string.Format(qryGetKeyLimits,
    //            srcTable[keyColName].NameQuoted,
    //            srcTable.FullTableName,
    //            (string.IsNullOrEmpty(whereClause)) ? "" : ("WHERE " + whereClause));
    //        using (SqlCommand cmdKeys = new SqlCommand(strKeyQuery, srcConn))
    //        {
    //            cmdKeys.CommandTimeout = 300; // timeout in seconds
    //            DataTable dtKeys = new DataTable();
    //            using (SqlDataReader rdrKeys = cmdKeys.ExecuteReader())
    //            {
    //                dtKeys.Load(rdrKeys);
    //            }

    //            if (dtKeys.Rows.Count > 0)
    //            {
    //                newFKB = new FetchKeyBoundary(srcTable, new List<string>() { keyColName });
    //                if (dtKeys.Rows[0]["MinKey"].GetType().Name != "DBNull") newFKB.StartKeyList.Add(dtKeys.Rows[0]["MinKey"]);
    //                if (dtKeys.Rows[0]["MaxKey"].GetType().Name != "DBNull") newFKB.EndKeyList.Add(dtKeys.Rows[0]["MaxKey"]);
    //            }
    //        }
    //        BackfillCtl.CloseDb(srcConn); // Close the DB connection
    //    }
    //    return newFKB;
    //}

    ///// <summary>
    ///// Create a single column boundary set
    ///// </summary>
    ///// <param name="srcTable">Reference to TableInfo object of the source table</param>
    ///// <param name="keyColName">Key column name</param>
    ///// <param name="minKey">Start value</param>
    ///// <param name="maxKey">Final value</param>
    ///// <returns></returns>
    //public static FetchKeyBoundary CreateFetchKeyBoundary(this TableInfo srcTable, string keyColName, object minKey, object maxKey)
    //{
    //    //TODO: Add multi-column key support

    //    FetchKeyBoundary newFKB = null;

    //    newFKB = new FetchKeyBoundary(srcTable, new List<string>(){ keyColName });
    //    if ((minKey != null) && (minKey.GetType().Name != "DBNull")) newFKB.StartKeyList.Add(minKey);
    //    if ((maxKey != null) && (maxKey.GetType().Name != "DBNull")) newFKB.StartKeyList.Add(maxKey);

    //    return newFKB;
    //}
}
