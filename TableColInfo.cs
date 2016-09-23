using System;
using System.Collections.Generic;
using System.Linq;

namespace DBBackfill
{
    public class TableColDatatype
    {
        public bool IsCopied = true;
        public bool IsComparible = true;
        public string ConvertFormat = string.Empty;
    }

    public class TableColInfo
    {
        //static private readonly Dictionary<string, bool> IgnoreDataTypes = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase) { { "TIMESTAMP", false } };
        //static private readonly Dictionary<string, bool> NoCompareTypes = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase) { { "TEXT", false } };

        static private readonly Dictionary<string, TableColDatatype> DatatypeInfo = new Dictionary<string, TableColDatatype>(StringComparer.InvariantCultureIgnoreCase){
            { "TEXT", new TableColDatatype(){ConvertFormat = "CONVERT(VARCHAR(MAX), {0})"} },
            { "NTEXT", new TableColDatatype(){ConvertFormat = "CONVERT(NVARCHAR(MAX), {0})"} },
            { "XML", new TableColDatatype(){ConvertFormat = "CONVERT(NVARCHAR(MAX), {0})"} },
            { "TIMESTAMP", new TableColDatatype(){IsCopied = false, IsComparible = false} }
        };   

        private bool _isIncluded = true;

        public string Name { get; set; }

        public string NameQuoted
        {
            get { return string.Format("[{0}]", Name); }
        }

        public string Datatype { get; set; }

        public int ID;
        public int MaxLength = 0;
        public int Precision = 0;
        public int Scale = 0;
        public int PartitionOrdinal = 0;
        public bool IsNullable = false;
        public bool IsIdentity = false;
        public bool IsComputed = false;
        public bool IsXmlDocument = false;

        public bool IsComparable
        { get { return (!DatatypeInfo.ContainsKey(Datatype)) || DatatypeInfo[Datatype].IsComparible; } }

        public bool IsCopyable  // True if this column is copied
        {
            get
            {
                return !DatatypeInfo.ContainsKey(Datatype) || DatatypeInfo[Datatype].IsCopied;
            }
        }

        public bool IsIncluded // Include column in backfill
        {
            get { return _isIncluded && IsCopyable; }
            set { _isIncluded = value; }
        }

        public int KeyOrdinal = 0;
        public bool KeyDescending = false;

        public string CmpValue(string prefix)
        {
            string refName = (string.IsNullOrEmpty(prefix)) ? NameQuoted : string.Format("{0}.{1}", prefix, NameQuoted);
            if (DatatypeInfo.ContainsKey(Datatype) && !string.IsNullOrEmpty(DatatypeInfo[Datatype].ConvertFormat))
                return string.Format(DatatypeInfo[Datatype].ConvertFormat, refName);
            else
                return refName;
        }
    }


    public class TableColInfoList : IEnumerable<TableColInfo>
    {
        private Dictionary<int, TableColInfo> _colList = new Dictionary<int, TableColInfo>();

        //  Indexers
        //
        public TableColInfo this[int colId]
        {
            get { return _colList.ContainsKey(colId) ? _colList[colId] : null; }
        }

        public TableColInfo this[string colName]
        {
            get { return _colList.Values.SingleOrDefault(ci => (string.Compare(ci.Name, colName, StringComparison.InvariantCultureIgnoreCase) == 0)); }
        }

        public void Add(TableColInfo newCol)
        {
            _colList.Add(newCol.ID, newCol);
        }

        //  Enumerable
        //
        public IEnumerator<TableColInfo> GetEnumerator()
        {
            return _colList.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _colList.Values.GetEnumerator();
        }
    }


}
