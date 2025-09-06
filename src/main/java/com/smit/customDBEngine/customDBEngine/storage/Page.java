package com.smit.customDBEngine.customDBEngine.storage;

import java.nio.ByteBuffer;

public abstract class Page {
	
	// This is the fixed page size i.e. 4KB = 4 * 1024 = 4096
	public static final int pageSize = 4096; 
	protected final int pageId;
	protected final ByteBuffer buf; 
	
	protected Page(int pageId, ByteBuffer buf) {
		this.pageId = pageId;
		this.buf = buf;
	}
	
	public int getPageId() {
		return pageId;
	}
	
	public ByteBuffer buffer() {
		return buf;
	}
	
	public abstract void initIfEmpty();

}
