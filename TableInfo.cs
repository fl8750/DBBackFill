using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;


namespace DBBackfill
{

    /// <summary>
    /// TableInfo -- Information about a table to be backfilled
    /// </summary>
    public class TableInfo : IEnumerable<TableColInfo>
    {
        //
        //  Table Properties
        //
        public string InstanceName { get; internal set; }
        public string DbName { get; internal set; }
        public string SchemaName { get; internal set; }
        public string TableName { get; internal set; }
        public bool Locked { get; set; }

        public string SchemaNameQuoted { get { return string.Format("[{0}]", SchemaName); } }
        public string TableNameQuoted { get { return string.Format("[{0}]", TableName); } }

        public string FullTableName
        { get { return string.Format("{0}.{1}", SchemaNameQuoted, TableNameQuoted); } }

        public int ObjectId { get; internal set; }
        public Int64 RowCount { get; set; }

        public bool HasIdentity { get; internal set; }

        //
        public bool IsPartitioned { get; set; }
        public string PtScheme { get; set; }
        public string PtFunc { get; set; }
        public TableColInfo PtCol { get; set; }

        private TableColInfoList _columns = new TableColInfoList();

        //
        //  Indexer
        //
        public TableColInfo this[string colName]
        {
            get { return _columns[colName]; }
        }

        public TableColInfo this[int colID]
        {
            get { return _columns[colID]; }
        }
        
        public Dictionary<int, TableColInfo> Columns
        {
            get { return _columns.Columns; }
        }

        public void AddColumn(TableColInfo newCol)
        {
            _columns.Add(newCol);
        }

        //  IEnumerable
        //
        public IEnumerator<TableColInfo> GetEnumerator()
        {
            return _columns.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _columns.GetEnumerator();
        }


        //
        //  Constructors
        //
        public TableInfo(string instanceName, string dbName, string schemaName, string tableName, int objectId, Int64 rowCount,  bool hasIdentity = false)
        {
            InstanceName = instanceName;
            DbName = dbName;
            SchemaName = schemaName;
            TableName = tableName;
            ObjectId = objectId;
            RowCount = rowCount;
            HasIdentity = hasIdentity;
            IsPartitioned = false;
            Locked = true;
        }

        public TableInfo()
        { }
    }


    /// <summary>
    /// TableInfo Collection class
    /// </summary>
    public class TableInfoList : IEnumerable<TableInfo>
    {
        //  Private information
        //
        //static private readonly Dictionary<string, bool> IgnoreDataTypes = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase) { { "TIMESTAMP", false } };
        //static private readonly Dictionary<string, bool> NoCompareTypes = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase) { { "TEXT", false } };        
        
        private Dictionary<int, TableInfo> _tables = new Dictionary<int, TableInfo>();

        //  Properties
        //
        public string InstanceName { get; private set; }
        public string DatabaseName { get; private set; }

        //  Indexers
        //
        public TableInfo this[int objectID]
        {
            get { return _tables.ContainsKey(objectID) ? _tables[objectID] : null; }
        }

        public TableInfo this[string schemaName, string tableName]
        {
            get
            {
                return _tables.Values.SingleOrDefault(ti => ((string.Compare(schemaName, ti.SchemaName, StringComparison.InvariantCultureIgnoreCase) == 0)
                  && (string.Compare(tableName, ti.TableName, StringComparison.InvariantCultureIgnoreCase) == 0)));
            }
        }


        //
        //  BuildTableInfo -- Retrieve information on the table from the database
        //
        private void BuildTableInfoList(SqlConnection dbConn, string dbName)
        {
            //  Now build a collection of tables and columns
            //            
            string strArticleColInfo = @"
    USE [{0}]; 
    WITH    IDXC
          AS ( 		  
			SELECT   *,
						ROW_NUMBER() OVER (PARTITION BY IDXC2.[object_id], IDXC2.column_id ORDER BY IDXC2.IndexPriority) AS ColPrio
               FROM     ( SELECT    DISTINCT
									TBL.[object_id] ,
                                    IDX.index_id ,
									IDX.[type],
                                    IDX.is_unique ,
                                    COL.column_id ,
                                    COALESCE(IDC.key_ordinal, 0) AS key_ordinal ,
                                    COALESCE(IDC.is_descending_key , 0) AS is_descending_key,
                                    COALESCE(IDC.partition_ordinal, 0) AS partition_ordinal
                                    ,DENSE_RANK() OVER ( PARTITION BY TBL.[object_id] ORDER BY IDX.is_unique DESC, IDX.index_id ) AS IndexPriority
	 FROM (sys.tables TBL 
		INNER JOIN sys.columns COL ON (TBL.[object_id] = COL.[object_id]))
		LEFT OUTER JOIN 
	(sys.indexes IDX
		INNER JOIN sys.index_columns IDC ON ( IDX.[object_id] = IDC.[object_id] )
                                                                        AND ( IDX.[index_id] = IDC.[index_id] ))
						ON (TBL.[object_id] = IDX.[object_id] ) AND (COL.column_id = IDC.column_id)
	WHERE TBL.[object_id] = 1554820601
                        ) IDXC2				
             )

         SELECT   TAB.[object_id] ,
                        TAB.[schema_id] ,
                        SCH.name AS SchemaName ,
                        TAB.name AS TableName ,
                        IDX.[type] AS index_type ,
                        IC.[name] AS ColName ,
                        IC.column_id ,
                        TYP.[name] AS DataType ,
                        IC.max_length ,
                        IC.[precision] ,
                        IC.[scale] ,
                        IC.is_xml_document ,
                        IC.is_nullable ,
                        IC.is_identity ,
                        IC.is_computed ,
                        COALESCE(IDXC.key_ordinal, 0) AS key_ordinal ,
                        COALESCE(IDXC.is_descending_key, 0) AS is_descending_key ,
                        CAST(IDXC.partition_ordinal AS INT) AS is_partitioned ,
                        COALESCE(IDXC.partition_ordinal, 0) AS partition_ordinal ,
                        COALESCE(PS.[name], '') AS PtScheme ,
                        COALESCE(PF.[name], '') AS PtFunc
               FROM     sys.tables TAB
                        INNER JOIN sys.schemas SCH ON ( TAB.[schema_id] = SCH.[schema_id] )
                        INNER JOIN sys.indexes IDX ON ( TAB.[object_id] = IDX.[object_id] )
                        INNER JOIN sys.columns IC ON ( TAB.[object_id] = IC.[object_id] )
                        INNER JOIN sys.types TYP ON ( IC.user_type_id = TYP.user_type_id )
                        LEFT OUTER JOIN /* sys.index_columns */ IDXC ON ( TAB.[object_id] = IDXC.[object_id] )
                                                                       -- AND ( IDX.index_id = IDXC.index_id )
                                                                        AND ( IC.column_id = IDXC.column_id )
                        LEFT JOIN ( sys.partition_schemes PS
                                    INNER JOIN sys.Partition_functions PF ON ( PS.function_id = PF.function_id )
                                  ) ON ( IDX.data_space_id = PS.data_space_id )
               WHERE    ( TAB.is_ms_shipped = 0 )
                        AND ( IDX.[type] IN ( 0, 1, 5 ) )
						AND (IDXC.ColPrio = 1)

               ORDER BY SchemaName ,
                        TableName ,
                        column_id;";

            DataTable dtTblColInfo = new DataTable();
            using (SqlCommand cmdArtcol = new SqlCommand(string.Format(strArticleColInfo, dbName), dbConn))
            {
                SqlDataReader srcRdr = cmdArtcol.ExecuteReader();
                dtTblColInfo.Load(srcRdr);
                srcRdr.Close();
            }

            foreach (DataRow cdr in dtTblColInfo.Rows)
            {
                string schemaName = (string) cdr["SchemaName"];
                string tableName = (string) cdr["TableName"];
                TableInfo curTbl = this[schemaName, tableName];
                if (curTbl == null)
                {
                    curTbl = new TableInfo(dbConn.DataSource, dbName, schemaName, tableName, (int) cdr["object_id"], 0 /*(Int64) cdr["RowCount"]*/);
                    _tables.Add(curTbl.ObjectId, curTbl);
                }

                //  Create the next table column information object
                //
                TableColInfo newCol =
                    new TableColInfo(){
                        Name = (string) cdr["ColName"],
                        Datatype = (string) cdr["Datatype"],

                        IsPsCol = ((int)cdr["partition_ordinal"]) > 0,

                        ID = (int) cdr["column_id"],
                        KeyOrdinal = (int) cdr["key_ordinal"],
                        MaxLength = (int) (Int16) (cdr["max_length"] ?? 0),
                        PartitionOrdinal = (int) (cdr["partition_ordinal"] ?? 0),
                        Precision = (int) (byte) (cdr["precision"] ?? 0),
                        Scale = (int) (byte) (cdr["scale"] ?? 0),

                        IsComputed = (bool) cdr["is_computed"],
                        IsIdentity = (bool) cdr["is_identity"],
                        IsNullable = (bool) cdr["is_nullable"],
                        IsXmlDocument = (bool) cdr["is_xml_document"],
                        KeyDescending = ((int)cdr["is_descending_key"] != 0)
                    };

                curTbl.AddColumn(newCol); // Add to the column list 

                if ((bool) cdr["is_identity"])
                    curTbl.HasIdentity = true; // Mark table as having an identity column

                if ((int) cdr["is_partitioned"] != 0)
                {
                    curTbl.IsPartitioned = true; // Mark table as "partitioned"
                    curTbl.PtScheme = (string) cdr["PtScheme"]; // Controlling scheme ...
                    curTbl.PtFunc = (string) cdr["PtFunc"]; // ... and function
                    if (newCol.IsPsCol)
                        curTbl.PtCol = newCol; // Save a reference to the partitioning column of this table
                }
            }

        }

        //  Enumerators
        //  
        public IEnumerator<TableInfo> GetEnumerator()
        {
            return _tables.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _tables.Values.GetEnumerator();
        }


        //  Constructors
        //
        public TableInfoList(SqlConnection dbConn, string dbName)
        {
            InstanceName = dbConn.DataSource;
            DatabaseName = dbConn.Database;
            BuildTableInfoList(dbConn, dbName);
        }
    }

}
