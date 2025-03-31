package buffermanager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Catalog implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Map<String, TableInfo> tables;
    private Map<String, IndexInfo> indexes;
    
    public Catalog() {
        this.tables = new HashMap<>();
        this.indexes = new HashMap<>();
    }
    
    public void addTable(String tableName, String filename, String[] schema) {
        tables.put(tableName, new TableInfo(tableName, filename, schema));
    }
    
    public TableInfo getTable(String tableName) {
        return tables.get(tableName);
    }
    
    public void addIndex(String indexName, String tableName, String searchKey, String filename) {
        indexes.put(indexName, new IndexInfo(indexName, tableName, searchKey, filename));
    }
    
    public IndexInfo getIndex(String indexName) {
        return indexes.get(indexName);
    }
    
    public Map<String, IndexInfo> getIndexes() {
        return indexes;
    }
    
    public static class TableInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String tableName;
        private String filename;
        private String[] schema;
        
        public TableInfo(String tableName, String filename, String[] schema) {
            this.tableName = tableName;
            this.filename = filename;
            this.schema = schema;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public String getFilename() {
            return filename;
        }
        
        public String[] getSchema() {
            return schema;
        }
    }
    
    public static class IndexInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String indexName;
        private String tableName;
        private String searchKey;
        private String filename;
        
        public IndexInfo(String indexName, String tableName, String searchKey, String filename) {
            this.indexName = indexName;
            this.tableName = tableName;
            this.searchKey = searchKey;
            this.filename = filename;
        }
        
        public String getIndexName() {
            return indexName;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public String getSearchKey() {
            return searchKey;
        }
        
        public String getFilename() {
            return filename;
        }
    }
}