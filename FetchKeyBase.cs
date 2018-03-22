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
        public List<object> StartKeyList { get; private set; }
        public List<object> EndKeyList { get; private set; }
        private List<object> _restartKeyList { get; set; }

        //public List<object> StartKeys_orig { get; private set; }
        //public List<object> EndKeys_orig { get; private set; }

        public BackfillType FillType = BackfillType.BulkInsert;

        //  Restart positioning information 
        //
        public bool FlgRestart = false; // Set true when initial restart check performed
        public int RestartPartition; // Restart partition number

        public List<object> RestartKeys // Keys used for restart
        {
            get
            {
                return _restartKeyList;
            }
            set
            {
                FlgRestart = true;
                _restartKeyList = value;
            }
        }

        public void AddRestartKey(object newKey)
        {
            _restartKeyList.Add(newKey);
        }

        public void AddEndKey(object newKey)
        {
            EndKeyList.Add(newKey);
        }

        //  Constructed SQL commands
        //
        public string FetchBatchSql = ""; // Get the next batch of rows
        public string FetchLastSql = ""; // Get the last row of the batch

        //  Methods
        //
        public virtual void BuildFetchQuery(SqlCommand srcCmd, TableInfo srcTable, int batchSize,
            int partNumber, bool isFirstFetch, List<string> copyColNames, List<string> keyColNames, List<object> curKeys)
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

            RestartPartition = 1;
            RestartKeys = new List<object>(); // Clear out the restart keys list
        }

    }

}
