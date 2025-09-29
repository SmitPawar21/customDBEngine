package com.smit.customDBEngine.customDBEngine;

import java.nio.file.Path;
import java.util.List;

import com.smit.customDBEngine.customDBEngine.catalog.Schema;
import com.smit.customDBEngine.customDBEngine.common.FieldType;
import com.smit.customDBEngine.customDBEngine.storage.BufferPool;
import com.smit.customDBEngine.customDBEngine.storage.DiskManager;
import com.smit.customDBEngine.customDBEngine.storage.HeapFile;
import com.smit.customDBEngine.customDBEngine.storage.Record;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        
        try {        	
        	DiskManager dm = new DiskManager(Path.of("data/mydb.db"));
        	BufferPool bp = new BufferPool(dm, 64);
        	HeapFile hf = new HeapFile(bp);
        	
        	Schema schema = new Schema(
        		List.of("id", "name"),
        		List.of(FieldType.INT, FieldType.STRING)
        	);
        	
        	hf.insert(new Record(schema, List.of(1, "Smit")));
        	hf.insert(new Record(schema, List.of(2, "Pawar")));
        	hf.insert(new Record(schema, List.of(3, "Diksha")));
        	hf.insert(new Record(schema, List.of(4, "Prajakti")));
        	
        	System.out.println("initial insertion: ");
        	for(Record r : hf.scan(schema)) {
        		System.out.println(r.asString());
        	}
        	
        	hf.update(0, 0, new Record(schema, List.of(9, "Vinod")));
        	
        	hf.delete(0, 1);
        	
        	System.out.println("After Updation and Deletion: ");
        	for(Record r : hf.scan(schema)) {
        		System.out.println(r.asString());
        	}
        	
        } catch (Exception e) {
        	System.out.println(e);
        }
        
    }
}
