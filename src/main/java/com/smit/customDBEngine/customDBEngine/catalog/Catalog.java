package com.smit.customDBEngine.customDBEngine.catalog;

import java.util.HashMap;
import java.util.Map;

import com.smit.customDBEngine.customDBEngine.storage.HeapFile;

public class Catalog {
	private final Map<String, Table> tables = new HashMap<>();

    public void addTable(String tableName, Schema schema, HeapFile file) {
        tables.put(tableName, new Table(tableName, schema, file));
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public Schema getSchema(String tableName) {
        return tables.get(tableName).getSchema();
    }

    public HeapFile getHeapFile(String tableName) {
        return tables.get(tableName).getHeapFile();
    }
    
    public static class Table {
        private final String name;
        private final Schema schema;
        private final HeapFile heapFile;

        public Table(String name, Schema schema, HeapFile heapFile) {
            this.name = name;
            this.schema = schema;
            this.heapFile = heapFile;
        }

        public Schema getSchema() { return schema; }
        public HeapFile getHeapFile() { return heapFile; }
    }
}
