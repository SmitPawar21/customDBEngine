package com.smit.customDBEngine.customDBEngine.storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

// THEORY
// LRU PRINCIPLE: The key principle is: items that haven't been used recently are less likely to be needed soon.

// This code uses a combination of HashMap [table]<pageId, Frame> + Deque [lru] for O(1) operations

public class BufferPool {
	private final DiskManager dm;
	private final int capacity;
	private final Map<Integer, Frame> table = new HashMap<>();
	private final Deque<Integer> lru = new ArrayDeque<>();
	
	private static class Frame {
		ByteBuffer buf;
		boolean dirty;
		Page page;
	}
	
	public BufferPool(DiskManager dm, int capacity) {
		this.dm = dm;
		this.capacity = capacity;
	}
	
	public synchronized Page getPage(int pageId) throws IOException {
		if(table.containsKey(pageId)) {
			touch(pageId);
			return table.get(pageId).page;
		}
		
		evictIfNeeded();
		ByteBuffer buf = dm.readPage(pageId);
		SlottedPage p = new SlottedPage(pageId, buf);
		
		Frame f = new Frame();
		f.buf = buf;
		f.page = p;
		
		table.put(pageId, f);
		
		lru.addFirst(pageId);
		return p;
	}
	
	public synchronized int newPage() throws IOException {
		int pid = dm.allocateNewPage();
		ByteBuffer buf = dm.readPage(pid);
		SlottedPage p = new SlottedPage(pid, buf);
		
		p.initIfEmpty();
		
		Frame f = new Frame();
		f.buf = buf;
		f.page = p;
		f.dirty = true;
		
		evictIfNeeded();
		
		table.put(pid, f);
		lru.addFirst(pid);
		
		return pid;
	}
	
	public synchronized void markDirty(int pageId) {
		table.get(pageId).dirty = true;
	}
	
	public synchronized void flushAll() throws IOException {
		for(var e: table.entrySet()) {
			flush(e.getKey(), e.getValue());
		}
		
		dm.force();
	}
	
	private void evictIfNeeded() throws IOException {
		if(table.size() < capacity) return;
		
		Integer victim = lru.removeLast();
		Frame f = table.remove(victim);
		
		flush(victim, f);
	}
	
	private void flush(int pageId, Frame f) throws IOException {
		if(f != null && f.dirty) {
			dm.writePage(pageId, f.buf);
			f.dirty=false;
		}
	}
	
	private void touch(int pageId) {
		lru.remove(pageId);
		lru.addFirst(pageId);
	}

}
