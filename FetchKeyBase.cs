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

        public bool IsValid // True if actual keys fetched
        {
            get { return ((StartKeyList.Count > 0) && (EndKeyList.Count > 0)); }
        }

        //  Source Table info
        //
        public TableInfo FKeySrcTable { get; protected set; } // Reference to information on the source table 
        public List<string> FKeyColNames { get; protected set; }
        public string SqlQuery = String.Empty;
        public bool FlgOrderBy = true;

        //  Range start and stop limits
        //
        private List<object> _startKeyList = new List<object>();
        private List<object> _endKeyList = new List<object>();

        public List<object> StartKeyList
        {
            get { return _startKeyList; }
        }

        public List<object> EndKeyList
        {
            get { return _endKeyList; }
        }

        //  Methods
        //
        public virtual string GetFetchQuery(TableInfo srcTable, SqlCommand cmdSrcDb, bool firstFetch, List<string> keyColNames, List<object> curKeys)
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
        }

    }

}
