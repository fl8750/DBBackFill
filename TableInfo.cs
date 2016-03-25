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
        WITH TABCNT AS (
                SELECT sOBJ.[object_id], 
                       SUM(sdmvPTNS.row_count) AS [RowCount]
                FROM  sys.objects AS sOBJ
                      INNER JOIN sys.dm_db_partition_stats AS sdmvPTNS
                            ON sOBJ.object_id = sdmvPTNS.object_id
                WHERE 
                      sOBJ.type = 'U'
                      AND sOBJ.is_ms_shipped = 0x0
                      AND sdmvPTNS.index_id < 2
                GROUP BY
                      sObj.[object_id]
                ),
            IdxInfo
              AS ( SELECT   *
                   FROM     ( SELECT    SCHEMA_NAME(OBJ.schema_id) AS SchemaName ,
                                        OBJ.Name AS TableName ,
                                        IDX.name AS IndexName ,
                                        OBJ.[object_id] ,
                                        IDX.index_id ,
                                        IDX.type_desc ,
                                        IDX.is_unique ,
                                        IDX.is_primary_key ,
                                        ROW_NUMBER() OVER ( PARTITION BY IDX.[object_id] ORDER BY IDX.is_unique DESC, IDX.index_id ) AS IndexPriority
                              FROM      sys.objects OBJ
                                        INNER JOIN sys.indexes IDX ON ( OBJ.[object_id] = IDX.[object_id] )
                              WHERE     ( OBJ.[type] = 'U' )
                            ) IINF
                   WHERE    IINF.IndexPriority = 1
                 ),
            ColInfo
              AS ( SELECT   INF.[object_id] ,
                            INF.index_id ,
                            IC.key_ordinal ,
                            IC.index_column_id ,
                            IC.column_id ,
                            IC.is_descending_key
                   FROM     IdxInfo INF
                            INNER JOIN sys.index_columns IC ON ( INF.[object_id] = IC.[object_id] )
                   WHERE    ( IC.key_ordinal <> 0 )
                            AND ( INF.index_id = IC.index_id )
                 ),
            IDC
              AS ( SELECT   INF.SchemaName ,
                            INF.TableName ,
                            INF.IndexName ,
                            INF.[object_id] ,
                            INF.index_id ,
                            INF.type_desc ,
                            INF.is_unique ,
                            INF.is_primary_key ,
                            SC.name AS ColName ,
                            CI.key_ordinal ,
                            CI.index_column_id ,
                            CI.column_id ,
                            CI.is_descending_key
                   FROM     IdxInfo INF
                            INNER JOIN ColInfo CI ON ( INF.[object_id] = CI.[object_id] )
                                                     AND ( INF.index_id = CI.index_id )
                            INNER JOIN sys.columns SC ON ( INF.[object_id] = SC.[object_id] )
                                                         AND ( SC.column_id = CI.column_id )
                 ),
            IDX
              AS ( SELECT   IDC.[object_id] ,
                            IDC.IndexName ,
                            MAX(IDC.index_id) AS index_id ,
                            MAX(IDC.type_desc) AS type_desc ,
                            MAX(CASE is_unique
                                  WHEN 0 THEN 0
                                  ELSE 1
                                END) AS is_unique
                   FROM     IDC
                   GROUP BY IDC.[object_id] ,
                            IDC.IndexName
                 )
        SELECT  OBJ.[object_id] ,
                SCHEMA_NAME(OBJ.schema_id) AS SchemaName ,
                OBJ.name AS TableName ,
                TABCNT.[RowCount] ,
                SC.name AS ColName ,
                ST.name AS [Datatype] ,
                SC.column_id ,
                SC.is_xml_document ,
                SC.max_length ,
                SC.[precision] ,
                SC.[scale] ,
                SC.is_nullable ,
                SC.is_identity ,
                SC.is_computed ,
                ISNULL(IDX.IndexName, '--') AS IndexName ,
                ISNULL(IDX.is_unique, 0) AS is_unique ,
                ISNULL(IDX.type_desc, '') AS idx_type ,
                ISNULL(IDC.key_ordinal, 0) AS key_ordinal ,
                ISNULL(IDC.is_descending_key, 0) AS is_descending_key
        FROM    sys.columns SC
                INNER JOIN sys.types ST ON ( SC.user_type_id = ST.user_type_id )
                INNER JOIN sys.objects OBJ ON ( SC.[object_id] = OBJ.[object_id] )
                INNER JOIN TABCNT ON (OBJ.[object_id] = TABCNT.[object_id])
                LEFT OUTER JOIN IDC ON ( SC.[object_id] = IDC.[object_id] )
                                       AND ( SC.name = IDC.ColName )
                LEFT OUTER JOIN IDX ON ( SC.[object_id] = IDX.[object_id] )
        WHERE   ( OBJ.[type] = 'U' )
        ORDER BY SchemaName, TableName, column_id";

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
                    curTbl = new TableInfo(dbConn.DataSource, dbName, schemaName, tableName, (int) cdr["object_id"], (Int64) cdr["RowCount"]);
                    _tables.Add(curTbl.ObjectId, curTbl);
                }

                TableColInfo newCol =
                    new TableColInfo(){
                        Name = (string) cdr["ColName"],
                        Datatype = (string) cdr["Datatype"],
                        ID = (int) cdr["column_id"],
                        MaxLength = (int) (Int16) (cdr["max_length"] ?? 0),
                        Precision = (int) (byte) (cdr["precision"] ?? 0),
                        Scale = (int) (byte) (cdr["scale"] ?? 0),
                        IsComputed = (bool) cdr["is_computed"],
                        IsIdentity = (bool) cdr["is_identity"],
                        IsNullable = (bool) cdr["is_nullable"],
                        IsXmlDocument = (bool) cdr["is_xml_document"],
                        KeyOrdinal = (int) (byte) cdr["key_ordinal"],
                        KeyDescending = (bool) cdr["is_descending_key"]
                    };
                curTbl.AddColumn(newCol);
                if ((bool) cdr["is_identity"])
                    curTbl.HasIdentity = true;
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
