using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;


namespace DBBackfill
{

    /// <summary>
    /// TableInfo -- Information about a table to be backfilled
    /// </summary>
    public class TableInfo : IEnumerable<TableColInfo>
    {

        public class TablePtInfo
        {
            public int PartitionNumber = 0;
            public Int64 Rows = 0;
        }

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
        public string ObjectType { get; internal set; }

        public enum enumObjType
        {
            Table = 0,
            View,
            Unknown = 9999999
        }
        
        public enumObjType ObjType
        {
            get
            {
                enumObjType result;
                if (enumObjType.TryParse(ObjectType, out result)) return result;
                return enumObjType.Unknown;
            }
        }

        public Int64 RowCount { get; set; }

        public bool HasIdentity { get; internal set; }

        public bool IsTable => ObjectType == "U";
        public bool IsView => ObjectType == "V";

        //  Table partitioning information
        //
        public bool IsPartitioned { get; set; }
        public string PtScheme { get; set; }
        public string PtFunc { get; set; }
        public int PtCount { get; set; }
        public TableColInfo PtCol { get; set; }

        public List<TablePtInfo> PtNotEmpty = new List<TablePtInfo>(); // Information on individual partitions in the table


        //  Column information
        //
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
        public TableInfo( SqlConnection dbConn, string instanceName, string dbName, string schemaName, string tableName, int objectId, string objType)
        {
            InstanceName = instanceName;
            DbName = dbName;
            SchemaName = schemaName;
            TableName = tableName;
            ObjectId = objectId;
            ObjectType = objType;
            HasIdentity = false; // Assume no identity column
            IsPartitioned = false;  // Assume no partitioning
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
        //
        //  Constants and strings
        //
        private string _sqlPartitionSizes = @"
        WITH PTR AS (
           SELECT	TAB.[object_id],
                                SCH.[name] AS SchemeName,
						        TAB.[name] AS TableName,
						        COALESCE(PS.[name],'') AS PtSchemeName,
						        COALESCE(PF.[name], '') AS PtFunctionName,
                                SUM(PT.[rows]) OVER ( PARTITION BY TAB.[object_id] ) AS TotalRows ,
                                COUNT(PT.partition_number) OVER ( PARTITION BY TAB.[object_id] ) AS TotalParts ,
                                PT.partition_number ,
                                PT.[rows]
                        FROM    sys.tables TAB
                                INNER JOIN sys.schemas SCH ON ( TAB.[schema_id] = SCH.[schema_id] )
                                INNER JOIN sys.indexes IDX ON ( TAB.[object_id] = IDX.[object_id] )
                                INNER JOIN sys.partitions PT ON ( TAB.[object_id] = PT.[object_id] )
                                                                AND ( IDX.index_id = PT.index_id )
					            LEFT JOIN ( sys.partition_schemes PS
                                            INNER JOIN sys.Partition_functions PF ON ( PS.function_id = PF.function_id )
                                          ) ON ( IDX.data_space_id = PS.data_space_id )
                        WHERE   ( IDX.index_id IN ( 0, 1 ) )
				        )
		        SELECT PTR.*,
			         COUNT(PTR.partition_number) OVER ( PARTITION BY PTR.[object_id] ) AS NotEmpty 
				        FROM PTR
                       WHERE (( PTR.[rows] > 0 ) OR (PTR.[PtSchemeName] = ''))
                        ORDER BY SchemeName ,
                                TableName ,
                                partition_number;";


        //  Get information on all the table columns
        //
        string strTableColInfo = @"
    USE [{0}]; 
    WITH IDXC
      AS
      (
          SELECT *,
                 ROW_NUMBER() OVER ( PARTITION BY IDXC2.[object_id],
                                                  IDXC2.column_id
                                     ORDER BY IDXC2.IndexPriority
                                   ) AS ColPrio
             FROM
                 (
                     SELECT DISTINCT
                            TBL.[object_id],
                            --OBJECT_SCHEMA_NAME(TBL.[object_id]) AS SchemaName,
                            --OBJECT_NAME(TBL.[object_id]) AS ObjectName,
                            TBL.[type] AS ObjType,
                            COALESCE(IDX.index_id, 0) AS IndexID,
                            --COALESCE(IDX.[type], 0) AS IndexType,
                            COALESCE(IDX.is_unique, 0) AS is_unique,
                            COL.column_id,
                            COALESCE(IDC.key_ordinal, 0) AS key_ordinal,
                            COALESCE(IDC.is_descending_key, 0) AS is_descending_key,
                            COALESCE(IDC.partition_ordinal, 0) AS partition_ordinal,
                            DENSE_RANK() OVER ( PARTITION BY TBL.[object_id]
                                                ORDER BY
                                                    COALESCE(IDX.is_unique, 0) DESC,
                                                    COALESCE(IDX.index_id, 0)
                                              ) AS IndexPriority
                        FROM(sys.objects TBL
                            INNER JOIN sys.columns COL
                               ON ( TBL.[object_id] = COL.[object_id] ))
                            LEFT OUTER JOIN(sys.indexes IDX
                            INNER JOIN sys.index_columns IDC
                               ON ( IDX.[object_id] = IDC.[object_id] )
                                  AND ( IDX.[index_id] = IDC.[index_id] ))
                              ON ( TBL.[object_id] = IDX.[object_id] )
                                 AND ( COL.column_id = IDC.column_id )
                        WHERE
                         ( TBL.[type] IN ( 'U', 'V' ))
					    --ORDER BY TBL.object_id, COL.column_id
                 ) IDXC2
		    --ORDER BY IDXC2.object_id, IDXC2.column_id, IDXC2.IndexPriority
      )
       SELECT TAB.[object_id],
              TAB.[schema_id],
              SCH.[name] AS SchemaName,
              TAB.[name] AS TableName,
              TAB.[type] AS ObjType,
              COALESCE(IDX.[type], 0) AS IndexType,
              IC.[name] AS ColName,
              IC.column_id,
              TYP.[name] AS DataType,
              IC.max_length,
              IC.[precision],
              IC.[scale],
              IC.is_xml_document,
              IC.is_nullable,
              IC.is_identity,
              IC.is_computed,
              COALESCE(IDXC.key_ordinal, 0) AS key_ordinal,
              COALESCE(IDXC.is_descending_key, 0) AS is_descending_key,
              COALESCE(IDXC.partition_ordinal, 0) AS partition_ordinal
          FROM sys.objects TAB
              INNER JOIN sys.schemas SCH
                 ON ( TAB.[schema_id] = SCH.[schema_id] )
              INNER JOIN sys.columns IC
                 ON ( TAB.[object_id] = IC.[object_id] )
              INNER JOIN sys.types TYP
                 ON ( IC.user_type_id = TYP.user_type_id )
              LEFT OUTER JOIN sys.indexes IDX
                ON ( TAB.[object_id] = IDX.[object_id] )
              LEFT OUTER JOIN /* sys.index_columns */ IDXC
                ON ( TAB.[object_id] = IDXC.[object_id] )
                   -- AND ( IDX.index_id = IDXC.index_id )
                   AND ( IC.column_id = IDXC.column_id )
          WHERE
           ( TAB.is_ms_shipped = 0 )
           AND ( TAB.[type] IN ( 'U', 'V' ))
           AND ( ISNULL(IDX.[type], 0) IN ( 0, 1, 5 ))
           AND ( IDXC.ColPrio = 1 )

          ORDER BY
           --[TAB].[object_id],
           SchemaName ,
           TableName ,
           IC.column_id;";



        //  Private information
        // 
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

            //  Fetch the list of tables/columns in this database
            //
            using (DataTable dtTblColInfo = new DataTable())
            {
                using (SqlCommand cmdArtcol = new SqlCommand(string.Format(strTableColInfo, dbName), dbConn))
                {
                    SqlDataReader srcRdr = cmdArtcol.ExecuteReader();
                    dtTblColInfo.Load(srcRdr);
                    srcRdr.Close();
                }

                //  Use LINQ grouping to split the dataset into tables and columns
                //
                var groupColsByTable = dtTblColInfo.AsEnumerable()
                                                   .GroupBy(tbl => (int)tbl["object_id"]);

                //  The outer "foreach" loop will iterate once per table.  The row group will contain all the columns for this table
                //
                foreach (var grpTable in groupColsByTable)
                {
                    int objectID = grpTable.Key;

                    //  The inner loop will loop once per column in this table
                    //
                    foreach (var dr in grpTable.OrderBy(col => col["column_id"]))
                    {
                        TableInfo curTbl = this[objectID]; 
                    
                        // Ensure that TableInfo object exists in our collection
                        if (curTbl == null) 
                        {
                            string schemaName = (string) dr["SchemaName"];
                            string tableName = (string) dr["TableName"];
                            string objType = (string)dr["ObjType"]; // "U"; or "V"
                            //string objType = (string)dr["ObjType"];
                            curTbl = new TableInfo(dbConn, dbConn.DataSource, dbName, schemaName, tableName, objectID, objType);
                            _tables.Add(curTbl.ObjectId, curTbl);
                        }

                        //  Now process each table column
                        TableColInfo newCol =
                            new TableColInfo()
                            {
                                Name = (string)dr["ColName"],
                                Datatype = (string)dr["Datatype"],

                                ID = (int)dr["column_id"],
                                KeyOrdinal = (int)dr["key_ordinal"],
                                MaxLength = (int)(Int16)(dr["max_length"] ?? 0),
                                PartitionOrdinal = (int)(dr["partition_ordinal"] ?? 0),
                                Precision = (int)(byte)(dr["precision"] ?? 0),
                                Scale = (int)(byte)(dr["scale"] ?? 0),

                                IsComputed = (bool)dr["is_computed"],
                                IsIdentity = (bool)dr["is_identity"],
                                IsNullable = (bool)dr["is_nullable"],
                                IsXmlDocument = (bool)dr["is_xml_document"],
                                KeyDescending = ((int)dr["is_descending_key"] != 0),

                                Ignore = false,
                                LoadExpression = "[__X__]"  // No custom value expression
                            };

                        curTbl.AddColumn(newCol); // Add to the column list 

                        //  Take care of the final table properties
                        if (newCol.IsIdentity)
                            curTbl.HasIdentity = true; // Mark table as having an identity column

                        if (newCol.IsPsCol)
                        {
                            curTbl.IsPartitioned = true; // Mark table as "partitioned"
                            curTbl.PtCol = newCol; // Save a reference to the partitioning column of this table
                        }
                    }
                }               
            }       
            
            //  Fetch partitioning info for these tables
            //            
            using (DataTable srcDt = new DataTable())
            {
                using (SqlCommand cmdPtSz = new SqlCommand(_sqlPartitionSizes, dbConn)) // Source database command
                {
                    cmdPtSz.CommandType = CommandType.Text;
                    cmdPtSz.CommandTimeout = 600;
                    using (SqlDataReader srcPtRdr = cmdPtSz.ExecuteReader()) // Fetch data into a data reader
                    {
                        srcDt.Load(srcPtRdr); // Load the data into a DataTable
                        srcPtRdr.Close();
                    }
                }

                var PtInfoByTable = srcDt.AsEnumerable().GroupBy(pt => pt["object_id"]);

                foreach (var PtInfo in PtInfoByTable)
                {
                    int objectID = (int)PtInfo.Key;  // Get the objectID for this table
                    TableInfo curTable = this[objectID];
                    if (curTable == null)
                        continue;
                        //throw new ApplicationException(string.Format("Unknown table: ID-[{0}]", objectID));

                    foreach (DataRow ptdr in PtInfo)
                    {
                        if (curTable.PtNotEmpty.Count == 0)
                        {
                            string ptScheme = (string)ptdr["PtSchemeName"];
                            if (string.IsNullOrEmpty(ptScheme))
                            {
                                curTable.IsPartitioned = false;
                                curTable.PtCount = 1;
                            }
                            else
                            {
                                curTable.IsPartitioned = true;
                                curTable.PtScheme = ptScheme;
                                curTable.PtFunc = (string)ptdr["PtFunctionName"];
                                curTable.PtCount = (int) ptdr["Totalparts"];
                            }
                            curTable.RowCount = (Int64) ptdr["TotalRows"];
                        }
                        curTable.PtNotEmpty.Add(new TableInfo.TablePtInfo()
                        {
                            PartitionNumber = (int)ptdr["partition_number"],
                            Rows = (Int64)ptdr["rows"]
                        });
                    }
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
