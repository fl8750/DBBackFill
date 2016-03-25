using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reflection.Emit;


namespace DBBackfill
{
    public class PublicationList : IEnumerable<Publication>
    {
        //  SQL Queries
        //
        private string strAllPubInfo = @"
IF EXISTS ( SELECT  *
            FROM    INFORMATION_SCHEMA.TABLES
            WHERE   TABLE_SCHEMA = N'dbo'
                    AND TABLE_NAME = N'syspublications' )
    BEGIN
        SELECT  CAST(SERVERPROPERTY('servername') AS VARCHAR(128)) AS ServerName ,
                ART.pubid ,
                PUB.name AS PubName,
                SCHEMA_NAME(OBJ.schema_id) AS srcSchemaName,
                OBJ.name AS srcTableName,
                ART.dest_owner AS dstSchemaName,
                ART.dest_table AS dstTableName,
                ART.artid ,
                ART.objid ,
                ACOL.colid AS column_id,
		        COL.name AS colName
            FROM    dbo.syspublications PUB
                INNER JOIN dbo.sysarticles ART  ON (PUB.pubid = ART.pubid)
                INNER JOIN dbo.sysarticlecolumns ACOL ON ( ART.artid = ACOL.artid )
                INNER JOIN sys.objects OBJ ON ( ART.objid = OBJ.[object_id] )
		        INNER JOIN sys.columns COL ON ( ART.objid = COL.[object_id] ) AND (ACOL.colid = COL.column_id)
            WHERE   ( OBJ.[type] = 'U' )
            ORDER BY pubid, artid, colid;
    END;
";

        private string strSubInfo = @"
    SELECT DISTINCT ART.[pubid], 
                    SUB.srvid, 
                    SUB.srvname, 
                    SUB.[dest_db], 
                    SUB.[distribution_jobid] 
        FROM dbo.sysarticles ART 
            INNER JOIN dbo.syssubscriptions SUB ON (ART.artid = SUB.artid) 
        ORDER BY pubid, srvid, dest_db;
";


        //  Private storage
        //
        private Dictionary<string, Publication> _publications = new Dictionary<string, Publication>(StringComparer.InvariantCultureIgnoreCase);

        //  Public storage
        //
        public Publication newPub = null;


        //  Indexers
        //
        public Publication this[string pubName]
        {
            get { return _publications[pubName]; }
        }

        public Publication this[int pubId]
        {
            get { return _publications.Values.SingleOrDefault(pb => (pb.PubID == pubId)); }
        }

        //  Methods
        //
        public void AddPublication(Publication newPub)
        {
            _publications.Add(newPub.Name, newPub);
        }

        //  Enumerators
        //
        public IEnumerator<Publication> GetEnumerator()
        {
            return _publications.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _publications.Values.GetEnumerator();
        }

        //
        //  Methods -- Setup the data structures needed fo a backfill on the source and destination side
        //
        private void LoadPublications(SqlConnection dbConn, DatabaseInfo dbInfo)
        {
            //  Retrieve all current publications
            //
            try
            {
                //  Create the initial list of Publication objects
                //
                dbConn.ChangeDatabase(dbInfo.DatabaseName); // Make sure we are directed to the proper database
                DataTable dtPubInfo = new DataTable();
                using (SqlCommand cmdArt = new SqlCommand(strAllPubInfo, dbConn))
                {
                    SqlDataReader rdrPubInfo = cmdArt.ExecuteReader();
                    dtPubInfo.Load(rdrPubInfo);
                    rdrPubInfo.Close();
                }

                if (dtPubInfo.Rows.Count == 0) return; // Exist immediately if no replication is present

                //  Build the nested publication / subscriber structure
                //
                DataTable dtSubscribers = new DataTable();
                using (SqlCommand cmdSub = new SqlCommand(strSubInfo, dbConn))
                {
                    SqlDataReader srcRdr = cmdSub.ExecuteReader();
                    dtSubscribers.Load(srcRdr);
                    srcRdr.Close();
                }

                var subs = dtSubscribers.AsEnumerable().GroupBy(psi => new{ PubID = psi.Field<int>("pubid") })
                                        .Select(ps => new{
                                            PubId = ps.Key.PubID,
                                            Subs = ps.Select(ps1 => new Subscriber(ps1.Field<string>("srvname"), 
                                                ps1.Field<string>("dest_db"), 
                                                new Guid(ps1.Field<byte[]>("distribution_jobid")).ToString()))
                                                .ToList()
                                        }).ToDictionary(psub => psub.PubId, psub => psub.Subs);

                //  Build the nested publication / article / column hierarchy structure
                //
                _publications = dtPubInfo.AsEnumerable().GroupBy(
                    rpi => new{
                        PubID = rpi.Field<int>("pubid"),
                        PubName = rpi.Field<string>("pubName")
                    })
                                    .Select(pb => new Publication(
                                       // dbConn,
                                        dbInfo,
                                        pb.Key.PubName,
                                        pb.Key.PubID,
                                        pb.GroupBy(ari => new{
                                            ArtID = ari.Field<int>("artid"),
                                            ObjID = ari.Field<int>("objid"),
                                            DstSchemaName = ari.Field<string>("dstSchemaName"),
                                            DstTableName = ari.Field<string>("dstTableName")
                                        })
                                          .Select(ar => new Article(
                                              ar.Key.ArtID,
                                              ar.Key.ObjID,
                                              ar.Key.DstSchemaName,
                                              ar.Key.DstTableName,
                                              ar.Select(cl => cl.Field<string>("colName")).ToList())).ToList(),
                                        subs[pb.Key.PubID]
                                        )).ToDictionary(pb => pb.Name, pb => pb);
            }
            catch (Exception ex)
            {
                throw;
            }
        }


        //  Constructor
        //
        public PublicationList(SqlConnection dbConn, DatabaseInfo dbInfo)
        {
            // Get List of publications
            //
            LoadPublications(dbConn, dbInfo);
        }
    }

}
