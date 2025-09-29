package com.smit.customDBEngine.customDBEngine;

import java.nio.file.Path;
import java.util.List;

import com.smit.customDBEngine.customDBEngine.catalog.Catalog;
import com.smit.customDBEngine.customDBEngine.catalog.Schema;
import com.smit.customDBEngine.customDBEngine.catalog.Catalog.Table;
import com.smit.customDBEngine.customDBEngine.common.FieldType;
import com.smit.customDBEngine.customDBEngine.storage.BufferPool;
import com.smit.customDBEngine.customDBEngine.storage.DiskManager;
import com.smit.customDBEngine.customDBEngine.storage.HeapFile;
import com.smit.customDBEngine.customDBEngine.storage.Record;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");

        try {
            // ---- Low-level setup ----
            DiskManager dm = new DiskManager(Path.of("data/mydb.db"));
            BufferPool bp = new BufferPool(dm, 64);

            // ---- Schema definition ----
            Schema userSchema = new Schema(
                List.of("id", "name"),
                List.of(FieldType.INT, FieldType.STRING)
            );

            // ---- Catalog & table registration ----
            Catalog catalog = new Catalog();
            HeapFile usersFile = new HeapFile(bp);
            catalog.addTable("users", userSchema, usersFile);

            // Get table handle from catalog
            Table users = catalog.getTable("users");
            HeapFile hf = users.getHeapFile(); // storage backend
            Schema schema = users.getSchema(); // schema

            // ---- Insert records ----
            hf.insert(new Record(schema, List.of(1, "Smit")));
            hf.insert(new Record(schema, List.of(2, "Pawar")));
            hf.insert(new Record(schema, List.of(3, "Diksha")));
            hf.insert(new Record(schema, List.of(4, "Prajakti")));

            // ---- Initial scan ----
            System.out.println("Initial insertion:");
            for (Record r : hf.scan(schema)) {
                System.out.println(r.asString());
            }

            // ---- Update & delete ----
            hf.update(0, 0, new Record(schema, List.of(9, "Vinod")));
            hf.delete(0, 1);

            // ---- Final scan ----
            System.out.println("After Updation and Deletion:");
            for (Record r : hf.scan(schema)) {
                System.out.println(r.asString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
