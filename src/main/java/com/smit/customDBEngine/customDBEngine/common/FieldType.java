package com.smit.customDBEngine.customDBEngine.common;

public enum FieldType {
	INT(4), STRING(-1);
	
	private final int fixedSize;
	
	FieldType(int fixedSize) {
		this.fixedSize = fixedSize;
	}
	
	public int getFixedSize() {
		return fixedSize;
	}
	
}
