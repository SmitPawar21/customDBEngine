package com.smit.customDBEngine.customDBEngine;

import java.nio.file.Path;

import com.smit.customDBEngine.customDBEngine.storage.BufferPool;
import com.smit.customDBEngine.customDBEngine.storage.DiskManager;
import com.smit.customDBEngine.customDBEngine.storage.HeapFile;
import com.smit.customDBEngine.customDBEngine.storage.Record;
/**
 * Hello world!2
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        
        try {        	
        	DiskManager dm = new DiskManager(Path.of("data/mydb.db"));
        	BufferPool bp = new BufferPool(dm, 64);
        	HeapFile hf = new HeapFile(bp);
        	
        	hf.insert("Smit".getBytes());
        	hf.insert("Pawar".getBytes());
        	
        	for(Record r : hf.scan()) {
        		System.out.println(r.asString());
        	}
        } catch (Exception e) {
        	System.out.println(e);
        }
        
    }
}
