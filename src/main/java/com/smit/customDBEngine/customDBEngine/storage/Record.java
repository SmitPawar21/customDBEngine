package com.smit.customDBEngine.customDBEngine.storage;

public class Record {
	
	private final byte[] data;
	
	public Record(byte[] data) {
		this.data = data;
	}
	
	public byte[] getBytes() {
		return data;
	}
	
	public String asString() {
		return new String(data);
	}

}
