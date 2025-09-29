package com.smit.customDBEngine.customDBEngine.catalog;

import java.util.List;

import com.smit.customDBEngine.customDBEngine.common.FieldType;

public class Schema {
	
	private final List<String> columnNames;
	private final List<FieldType> columnTypes;
	
	public Schema(List<String> columnNames, List<FieldType> columnTypes) {
		if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Mismatched names and types");
        }
		
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
	}
	
	public int numColumns() {
        return columnNames.size();
    }

    public String getColumnName(int i) {
        return columnNames.get(i);
    }

    public FieldType getColumnType(int i) {
        return columnTypes.get(i);
    }

}
