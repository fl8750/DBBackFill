using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;


namespace DBBackfill
{

    public class FetchKeyBase
    {
        //  Fetch Key Information
        //
        public string Name { get; protected set; } //  Name of this FetchKey

        //  Source Table info
        //
        public TableInfo FKeySrcTable { get; protected set; } // Reference to information on the source table 
        public List<string> FKeyColNames { get; protected set; }

        public List<TableColInfo> FKeyDataCols = new List<TableColInfo>();  // Collection of data columns to be fetched and moved

        //  Destination Table Info
        //
        //public TableInfo FKeyDstTable { get; protected set; }

        //  Boolean properties
        //
        public bool FlgOrderBy = true;
        public bool FlgSelectByPartition = false; // If true, Fetch using partition number as first key

        //  Range start and stop limit lists
        //
        public List<object> StartKeyList { get; private set; }
        public List<object> EndKeyList { get; private set; }
        public List<object> RestartKeyList { get; private set; }

        //  Backfill 
        private BackfillType _fillType;

        public BackfillType FillType
        {
            get => _fillType;
            set => _fillType = value;
        }

        public string FillTypeName
        {
            get => _fillType.ToString(); // Get the BackfillType name
            set => _fillType = (BackfillType) Enum.Parse(typeof(BackfillType), value);
        }

        //  Restart positioning information 
        //
        public bool FlgRestart // Set true when initial restart check performed
        {
            get;
            protected set;
        }

        private int _restartPartition = 0;

        public int RestartPartition // Restart partition number
        {
            get => _restartPartition;
            set
            {
                _restartPartition = value;
                FlgRestart = (_restartPartition >= 1); // Set the restart flag accordingly
            }
        }


        public void AddRestartKey(object newKey)
        {
            RestartKeyList.Add(newKey);
            FlgRestart = true; // Mark as a restart
        }

        public void AddEndKey(object newKey)
        {
            EndKeyList.Add(newKey);
        }


        //  Constructed SQL commands
        //
        public string FetchKeyLimitsSql = ""; // Fetch th keys of the last row in the next fetch row group
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
        public FetchKeyBase(TableInfo srcTable, List<string> keyColNames, TableInfo dstTable = null)
        {
            //  Set up the source table information
            //
            FKeySrcTable = srcTable;
            FKeyColNames = keyColNames;
            if (keyColNames == null)
            {
                FKeyColNames = srcTable.Where(kc => kc.KeyOrdinal > 0).OrderBy(kc => kc.KeyOrdinal).Select(kc => kc.Name).ToList();
            }

            StartKeyList = new List<object>(); // Initialize the start/end key lists
            EndKeyList = new List<object>();

            //  Setup any destination table information
            //
            FKeyDstTable = dstTable;

            FillType = BackfillType.BulkInsert; // Default to bulk insert

            FlgRestart = false; // Assume no restart at this point
            RestartKeyList = new List<object>(); // Clear out the restart keys list

            //  Check for any partitioning performance problems
            //
            RestartPartition = 1; // Default to the irst partition
            FlgSelectByPartition = false; // Assume not partitioned
            if ((srcTable.IsPartitioned) && (srcTable.PtCol.ID == srcTable[keyColNames[0]].ID))
            {
                FlgSelectByPartition = true;
            }
        }

        public FetchKeyBase(TableInfo srcTable, string keyColNames, TableInfo dstTable = null)
            : this(srcTable, keyColNames?.Split(',').ToList(), dstTable) { }

    }

}
