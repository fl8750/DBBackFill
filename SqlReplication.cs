using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DBBackfill
{
    public class PublicationList : IEnumerable<Publication>
    {
        private Dictionary<string, Publication> _publications = new Dictionary<string, Publication>(StringComparer.InvariantCultureIgnoreCase);

        //  Indexers
        //
        public Publication this[string pubName]
        {
            get { return _publications[pubName]; }
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

        //  Constructor
        //
        public PublicationList(DatabaseInfo dbInfo, SqlConnection dbConn)
        {
            // Get List of publications
            //
        }
    }


    /// <summary>
    /// Publication -- Information about a database publication
    /// </summary>
    public class Publication
    {
        public DatabaseInfo DbInfo { get; private set; }
        public string Name { get; private set; }
        public int PubID { get; private set; }
        public string ServerName { get; private set; }
        public string DbName { get; private set; }

        public ArticleCollection Articles = new ArticleCollection(); // List of article tables in the publiication
        public List<Subscriber> Subscribers = new List<Subscriber>(); // List of defined subscribers

        //
        //  Methods
        //
        public void AddArticle(string schemaName, string tableName, int artId, int objId)
        {
            Article newArt = new Article(schemaName, tableName, artId, objId);
            Articles.Add(newArt);
        }

        public void AddSubscriber(string instanceName, string dstDbNam, string distribJobId)
        {
            Subscriber newSub = new Subscriber(){
                InstanceName = instanceName,
                DbName = dstDbNam,
                DistribJob = distribJobId
            };
            Subscribers.Add(newSub);
        }


        //  OpenSubScriber -- Open a connection to the subscribing SQL instance
        //
        //public Subscriber OpenSubscriber(string dstServer, string dstDbName)
        //{
        //    foreach (Subscriber subcr in Subscribers)
        //    {
        //        if ((string.Compare(dstServer, subcr.InstanceName, StringComparison.InvariantCultureIgnoreCase) == 0)
        //            && (string.Compare(dstDbName, subcr.DbName, StringComparison.InvariantCultureIgnoreCase) == 0))
        //        {
        //            subcr.Open();
        //            if (!this.ValidSubscription(subcr))
        //                throw new ApplicationException(string.Format("Not a valid subscriber: [{0}] - [{1}]", dstServer, dstDbName));
        //            return subcr;
        //        }
        //    }
        //    throw new ApplicationException(string.Format("Unknown subscriber: [{0}].[{1}]", dstServer, dstDbName));
        //}

        //public void Open()
        //{
        //    DbConnString = string.Format("server={0};database={1};trusted_connection=true;", ServerName, DbName);
        //    DbConn = new SqlConnection(DbConnString);
        //    DbConn.Open();
        //}


        //
        //  Constructors
        //
        public Publication(string serverName, string dbName, string pubName, int pubId)
        {
            ServerName = serverName;
            DbName = dbName;
            Name = pubName;
            PubID = pubId;
           // Open();
        }

        public Publication() {}
    }


    /// <summary>
    /// 
    /// </summary>
    public class ArticleCollection: IEnumerable<Article>
    {
        private List<Article> _articles = new List<Article>();

        //
        //  Article Indexer
        //
        public Article this[string articleSchema, string articleTable]
        {
            get
            {
                return _articles.SingleOrDefault(x => ((string.Compare(x.SchemaName, articleSchema, StringComparison.InvariantCultureIgnoreCase) == 0)
                                                      && (string.Compare(x.TableName, articleTable, StringComparison.InvariantCultureIgnoreCase) == 0)));
            }
        }

        public Article this[int artId]
        {
            get { return _articles.SingleOrDefault(x => (x.ArtID == artId)); }
        }

        //
        // Methods
        //
        public void Add(Article newArticle)
        {
            _articles.Add(newArticle);
        }
             
        public ArticleCollection() {}

        public IEnumerator<Article> GetEnumerator()
        {
            return _articles.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _articles.GetEnumerator();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class Article
    {
        public TableInfo Table = null;
        public int ArtID { get; private set; }
        public string ObjType { get; private set; }
        public string SchemaName { get; private set; }
        public string TableName { get; private set; }
        public int ObjID { get; private set; }

        public List<string> Columns = new List<string>();


        public void AddColumn(string colName)
        {
            Columns.Add(colName);
        }

        public Article(string schemaName, string tableName, int artId, int objId)
        {
            SchemaName = schemaName;
            TableName = tableName;
            ArtID = artId;
            ObjID = objId;
        }
    }


    /// <summary>
    /// 
    /// </summary>
    public class Subscriber
    {
        public string InstanceName = string.Empty;
        public string DbName = string.Empty;
        public string DistribJob = string.Empty;

        public string DbConnString { get; private set; }
        public SqlConnection DbConn { get; private set; }


        //  Methods -- Database connection 
        //
        //public void Open()
        //{
        //    DbConnString = string.Format("server={0};database={1};trusted_connection=true;", InstanceName, DbName);
        //    DbConn = new SqlConnection(DbConnString);
        //    DbConn.Open();
        //}

        //public void Close()
        //{
        //    if (DbConn != null)
        //    {
        //        if (DbConn.State != ConnectionState.Closed)
        //        {
        //            DbConn.Close();
        //            DbConn.Dispose();
        //        }
        //        DbConn = null;
        //    }
        //}
    }

}
