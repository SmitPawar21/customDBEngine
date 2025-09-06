package com.smit.customDBEngine.customDBEngine.storage;
import java.io.*;
import java.util.*;

public class HeapFile {
	
	private final BufferPool bp;
	private final List<Integer> pages = new ArrayList<>();
	
	public HeapFile (BufferPool bp) {
		this.bp = bp;
	}	
	
	public void insert (byte[] record) throws IOException {
		if(pages.isEmpty()) {
			pages.add(bp.newPage());
		}
		
		SlottedPage p = (SlottedPage) bp.getPage(pages.get(pages.size() - 1));
		
		boolean success = p.insertRecord(record);
		
		if(!success) {
			// This means not enough space
			
			int newPid = bp.newPage();
			pages.add(newPid);
			
			SlottedPage newPage = (SlottedPage) bp.getPage(newPid);
			if(! newPage.insertRecord(record)) {
				// False even after adding a new page then record is too large
				throw new IOException("Record Too Large to fit in empty space");
			}
			
			bp.markDirty(newPid);
		} else {
			bp.markDirty(pages.get(pages.size() - 1));
		}
	}
	
	

}
