using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBBackfill
{
    public static class SqlReplicationHelpers
    {

        //
        //  Methods -- Setup the data structures needed fo a backfill on the source and destination side
        //
        public static Publication OpenPublication(DatabaseInfo dbInfo, string pubName)
        {
            Publication newPub = null;

            using (SqlConnection dbConn = BackfillCtl.OpenDB(dbInfo.InstanceName, dbInfo.DatabaseName))
            {
                //  Retrieve all current publications
                //
                try
                {
                    using (
                        SqlCommand cmdPubs =
                            new SqlCommand(string.Format("SELECT CAST(SERVERPROPERTY('servername') AS VARCHAR(128)) AS ServerName, * FROM [{0}].dbo.syspublications WHERE [name] = '{1}';",
                                dbInfo.DatabaseName,
                                pubName),
                                dbConn))
                    {
                        SqlDataReader srcRdr = cmdPubs.ExecuteReader();
                        DataTable srcDt = new DataTable();
                        srcDt.Load(srcRdr);
                        srcRdr.Close();
                        if (srcDt.Rows.Count != 1)
                            throw new ApplicationException(string.Format("Cannot find publication: '{0}'", pubName));

                        newPub = new Publication((string) srcDt.Rows[0]["ServerName"], dbInfo.DatabaseName, pubName, (int) srcDt.Rows[0]["pubid"]);
                    }
                }
                catch (Exception ex)
                {
                    throw;
                }

                //  Retrieve the article List -- Only include user created tables
                //
                string strArticleInfo = @"USE [{0}]; 
                                      SELECT SCHEMA_NAME(ART.objid) AS schemaName,
                                             ART.* 
                                        FROM dbo.sysarticles ART
                                            INNER JOIN sys.objects OBJ ON (ART.objid = OBJ.[object_id]) 
                                        WHERE   (ART.[pubid] = {1}) AND (OBJ.[type] = 'U'); ";
                try
                {
                    using (SqlCommand cmdArt = new SqlCommand(string.Format(strArticleInfo, dbInfo.DatabaseName, newPub.PubID), dbConn))
                    {
                        SqlDataReader srcRdr = cmdArt.ExecuteReader();
                        DataTable dtArticles = new DataTable();
                        dtArticles.Load(srcRdr);
                        srcRdr.Close();

                        //  Spin through all the article tables in the publication
                        //
                        foreach (DataRow adr in dtArticles.Rows)
                        {
                            newPub.AddArticle((string) adr["dest_owner"], (string) adr["dest_table"], (int) adr["artid"], (int) adr["objid"]);
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }

                //  Retrieve each article's columns List
                //
                string strArticleColInfo = @"USE [{0}]; 
                                        SELECT  ART.pubid ,
                                                ART.artid ,
                                                ART.objid ,
                                                ACOL.colid AS column_id,
		                                        COL.name AS colName
                                            FROM    dbo.sysarticles ART
                                                INNER JOIN dbo.sysarticlecolumns ACOL ON ( ART.artid = ACOL.artid )
                                                INNER JOIN sys.objects OBJ ON ( ART.objid = OBJ.[object_id] )
		                                        INNER JOIN sys.columns COL ON ( ART.objid = COL.[object_id] ) AND (ACOL.colid = COL.column_id)
                                            WHERE   ( ART.[pubid] = {1} )
                                                 AND ( OBJ.[type] = 'U' );";
                try
                {
                    using (SqlCommand cmdArtcol = new SqlCommand(string.Format(strArticleColInfo, dbInfo.DatabaseName, newPub.PubID), dbConn))
                    {
                        SqlDataReader srcRdr = cmdArtcol.ExecuteReader();
                        DataTable dtArtColumns = new DataTable();
                        dtArtColumns.Load(srcRdr);
                        srcRdr.Close();

                        foreach (DataRow acdr in dtArtColumns.Rows)
                        {
                            //Article curArt = newPub.Articles[(string)acdr["dest_owner"], (string)acdr["dest_table"]];
                            Article curArt = newPub.Articles[(int) acdr["artid"]];
                            if (curArt != null)
                                curArt.AddColumn((string) acdr["colName"]);
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }


                //  Retrieve the subscriber List
                //
                string strSubInfo = @"SELECT DISTINCT SUB.srvid, SUB.srvname, SUB.[dest_db], SUB.[distribution_jobid] 
                                    FROM [{0}].dbo.sysarticles ART INNER JOIN [{0}].dbo.syssubscriptions SUB ON (ART.artid = SUB.artid) 
                                        WHERE ART.[pubid] = {1};";
                try
                {
                    using (SqlCommand cmdSub = new SqlCommand(string.Format(strSubInfo, dbInfo.DatabaseName, newPub.PubID), dbConn))
                    {
                        SqlDataReader srcRdr = cmdSub.ExecuteReader();
                        DataTable dtSubscribers = new DataTable();
                        dtSubscribers.Load(srcRdr);
                        srcRdr.Close();

                        foreach (DataRow sdr in dtSubscribers.Rows)
                        {
                            newPub.AddSubscriber((string) sdr["srvname"], (string) sdr["dest_db"], new Guid((byte[]) sdr["distribution_jobid"]).ToString());
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }

                BackfillCtl.CloseDb(dbConn);
            }

            return newPub;
        }


        //
        //  ClosePublication -- Close database connections and release resources
        //
        public static void ClosePublication(this BackfillCtl bkflCtl)
        {
            ;
        }


        // ==============================================================================================================
        //
        //  ValidSubscription -- Verify this subscription is part of this publication
        //
        public static bool ValidSubscription(this Publication pub, Subscriber sub)
        {
            //  Retrieve the subscriber List
            //
            string strSubInfo = @"SELECT COUNT(*) AS SubCount
                                  FROM [{0}].[dbo].[MSreplication_subscriptions]
                                  WHERE (publisher = '{1}') AND (publication = '{2}') ";
            try
            {
                using (SqlCommand cmdSub = new SqlCommand(string.Format(strSubInfo, sub.DbName, pub.ServerName, pub.Name), sub.DbConn))
                {
                    int subCount = (int) cmdSub.ExecuteScalar();
                    return (subCount == 1);
                }
            }
            catch (Exception)
            {
                throw;
            }

        }

    }
}
