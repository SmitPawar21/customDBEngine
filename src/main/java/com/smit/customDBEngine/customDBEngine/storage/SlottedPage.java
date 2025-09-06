package com.smit.customDBEngine.customDBEngine.storage;

import java.nio.ByteBuffer;

public class SlottedPage extends Page {
	private static final int headerSize = 8; // numSlots(4) + freePtr(4)
	
	public SlottedPage(int pageId, ByteBuffer buf) {
		super(pageId, buf);
	}
	
	@Override
	public void initIfEmpty() {
		if(buf.capacity() != pageSize) throw new IllegalStateException();
		
		buf.clear();
		buf.putInt(0, 0);			// numSlots
		buf.putInt(4, pageSize); 	// freePtr
	}
	
	public static record Slot(int offset, int len, boolean tombstone) {}
	
	public boolean insertRecord (byte[] data) {
		int required = data.length + 4;
		int numSlots = buf.getInt(0);
		int freePtr = buf.getInt(4);
		
		int slotDirEnd = headerSize + numSlots * 8;
		int freeSpace = freePtr - slotDirEnd;
		
		if(freeSpace < required) {
			return false;
		}
		
		int newFreePtr = freePtr - data.length;
		buf.position(newFreePtr);
		buf.put(data);
		
		buf.putInt(headerSize + numSlots * 8, newFreePtr);		// offset
		buf.putInt(headerSize + numSlots * 8 + 4, data.length); // length
		
		buf.putInt(0, numSlots + 1);
		buf.putInt(4, newFreePtr);
		
		return true;
	}
	
}
