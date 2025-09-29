package com.smit.customDBEngine.customDBEngine.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.smit.customDBEngine.customDBEngine.catalog.Schema;
import com.smit.customDBEngine.customDBEngine.common.FieldType;

public class Record {
	
	private final Schema schema;
	private final List<Object> values;
	
	public Record(Schema schema, List<Object> values) {
		this.schema = schema;
		this.values = values;
	}
	
	public Object getValue(int i) {
        return values.get(i);
    }

    public String getValueAsString(int i) {
        return String.valueOf(values.get(i));
    }
    
    public String asString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.numColumns(); i++) {
            sb.append(schema.getColumnName(i))
              .append("=")
              .append(getValueAsString(i));
            if (i < schema.numColumns() - 1) sb.append(", ");
        }
        return sb.toString();
    }
    
    public byte[] serialize() {
        ByteBuffer buf = ByteBuffer.allocate(1024); 
        // TODO: dynamic sizing

        for (int i = 0; i < schema.numColumns(); i++) {
            FieldType type = schema.getColumnType(i);
            Object val = values.get(i);

            switch (type) {
                case INT -> buf.putInt((Integer) val);
                case STRING -> {
                    byte[] str = ((String) val).getBytes(StandardCharsets.UTF_8);
                    buf.putInt(str.length);  
                    buf.put(str);
                }
            }
        }

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);
        return data;
    }
    
    public static Record deserialize(Schema schema, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        List<Object> values = new ArrayList<>();

        for (int i = 0; i < schema.numColumns(); i++) {
            FieldType type = schema.getColumnType(i);
            switch (type) {
                case INT -> values.add(buf.getInt());
                case STRING -> {
                    int len = buf.getInt();
                    byte[] strBytes = new byte[len];
                    buf.get(strBytes);
                    values.add(new String(strBytes, StandardCharsets.UTF_8));
                }
            }
        }
        return new Record(schema, values);
    }

}
