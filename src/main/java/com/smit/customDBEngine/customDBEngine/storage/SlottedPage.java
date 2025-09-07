package com.smit.customDBEngine.customDBEngine.storage;
import java.util.*;
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
	
	public byte[] getRecord (int slotId) {
		int numSlots = buf.getInt(0);
		if(slotId < 0 || slotId >= numSlots) {
			throw new IllegalArgumentException("Invalid slot id: "+ slotId);
		}
		
		int slotPos = headerSize + slotId * 8;
		int offset = buf.getInt(slotPos);
		int len = buf.getInt(slotPos + 4);
		
		if(offset == -1) {
			return null;
		}
		
		byte[] data = new byte[len];
		buf.position(offset);
		buf.get(data, 0, len);
		return data;
	}
	
	public List<byte[]> getAllRecords () {
		int numSlots = getNumSlots();
		List<byte[]> records = new ArrayList<>(Math.max(0, numSlots));
		
		for(int i=0; i<numSlots; i++) {
			byte[] rec = getRecord(i);
			if(rec != null) {
				records.add(rec);
			}
		}
		
		return records;
	}
	
	public void deleteRecord(int slotId) {
		int numSlots = buf.getInt(0);
		if(slotId < 0 || slotId >= numSlots) {
			throw new IllegalArgumentException("Invalid slot id: "+ slotId);
		}
		
		int slotPos = headerSize + slotId * 8;
		buf.putInt(slotPos, -1);
		buf.putInt(slotPos + 4, 0);
	}
	
	public boolean hasFreeSpace(int size) {
		int numSlots = buf.getInt(0);
		int freeptr = buf.getInt(4);
		
		int slotDirEnd = headerSize + numSlots * 8;
		int required = size + 8;
		int freespace = freeptr - slotDirEnd;
		
		return freespace >= required;
	}
	
	public int getNumSlots () {
		return buf.getInt(0);
	}
	
}
