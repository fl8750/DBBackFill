using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;


namespace DBBackfill
{

    public class FetchKeyBase
    {
        //  Fetch Key Information
        //
        public string Name { get; protected set; } //  Name of this FetchKey

        public bool IsValid
        {
            get { return ((StartKeyList.Count > 0) && (EndKeyList.Count > 0)); } // True if actual keys fetched
        }

        //  Source Table info
        //
        public TableInfo FKeySrcTable { get; protected set; } // Reference to information on the source table 
        public List<string> FKeyColNames { get; protected set; }

        //  Boolean properties
        //
        public bool FlgOrderBy = true;
        public bool FlgSelectByPartition = false; // If true, Fetch using partition number as first key

        //  Range start and stop limit lists
        //
        public int PartitionIdx = 0; // Current partition number

        public List<object> StartKeyList { get; private set; }
        public List<object> EndKeyList { get; private set; }

        public List<object> StartKeys_orig { get; private set; }
        public List<object> EndKeys_orig { get; private set; }

        //  Methods
        //
        public virtual string GetFetchQuery(TableInfo srcTable, SqlCommand cmdSrcDb, int partNumber, int fetchCnt, List<string> keyColNames, List<object> curKeys)
        {
            throw new ApplicationException("Not Implemented!");
        }

        public virtual List<object> FetchNextKeyList(DataRow lastDataRow)
        {
            throw new ApplicationException("Not Implemented!");
        }


        //
        //  Constructors
        //
        public FetchKeyBase(TableInfo srcTable, List<string> keyColNames)
        {
            FKeySrcTable = srcTable;
            FKeyColNames = keyColNames;

            StartKeyList = new List<object>(); // Initialize the start/end key lists
            EndKeyList = new List<object>();
        }

    }

}
