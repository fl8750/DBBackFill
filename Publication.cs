using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DBBackfill
{

    /// <summary>
    /// Publication -- Information about a database publication
    /// </summary>
    public class Publication
    {
        public string Name { get; private set; }
        public int PubID { get; private set; }
        public string ServerName { get; private set; }
        public DatabaseInfo DBInfo { get; private set; }

        public ArticleCollection Articles = new ArticleCollection(); // List of article tables in the publiication
        public List<Subscriber> Subscribers = new List<Subscriber>(); // List of defined subscribers

        //
        //  Methods
        //

        //
        //  Constructors
        //
        public Publication(DatabaseInfo dbInfo, string pubName, int pubId, List<Article> articles, List<Subscriber> subscribers )
        {
            ServerName = dbInfo.InstanceName;
            DBInfo = dbInfo;
            Name = pubName;
            PubID = pubId;

            Articles = new ArticleCollection(this, articles);
            Subscribers = subscribers;
        }
    }


    /// <summary>
    /// 
    /// </summary>
    public class ArticleCollection : IEnumerable<Article>
    {
        private List<Article> _articles = new List<Article>();

        //
        //  Article Indexer
        //
        public Article this[string articleSchema, string articleTable]
        {
            get
            {
                return _articles.SingleOrDefault(x => ((string.Compare(x.Table.SchemaName, articleSchema, StringComparison.InvariantCultureIgnoreCase) == 0)
                                                       && (string.Compare(x.Table.TableName, articleTable, StringComparison.InvariantCultureIgnoreCase) == 0)));
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

        public IEnumerator<Article> GetEnumerator()
        {
            return _articles.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _articles.GetEnumerator();
        }

        //  Constructors
        //
        public ArticleCollection(Publication newPub, List<Article> articles )
        {
            foreach (Article newArt in articles)
            {
                newArt.Table = newPub.DBInfo[newArt.ObjID];  // Insert reference to the source database TableInfo object
                _articles.Add(newArt);
            }
        }

        public ArticleCollection() {}
    }



    /// <summary>
    /// 
    /// </summary>
    public class Article
    {
        public TableInfo Table = null;
        public int ArtID { get; private set; }
        public string ObjType { get; private set; }
        public int ObjID { get; private set; }
        public string DstSchemaName { get; private set; }
        public string DstTableName { get; private set; }

        public List<string> Columns = new List<string>();

        public void AddColumn(string colName)
        {
            Columns.Add(colName);
        }

        public Article(int artId, int objId, string dstSchemaName, string dstTableName, List<string> columns) 
        {
            ArtID = artId;
            ObjID = objId;
            DstSchemaName = dstSchemaName;
            DstTableName = dstTableName;
            Columns = columns;
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

        public Subscriber(string instanceName, string dbName, string distribJob)
        {
            InstanceName = instanceName;
            DbName = dbName;
            DistribJob = distribJob;
        }
    }

}
