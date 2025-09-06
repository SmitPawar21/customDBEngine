package com.smit.customDBEngine.customDBEngine.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public class DiskManager implements AutoCloseable {
	
	// channel is an object of FileChannel 
	// channel is connection or "pipe" to a file on disk
	// Reason for having this variable as "final" - this is a permanent connection to ONE file. Also can't accidentally point to wrong file
	private final FileChannel channel;
	
	public DiskManager (Path path) throws IOException {
		channel = FileChannel.open(path, 
					StandardOpenOption.CREATE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE
				);	
	}
	
	// READING PAGE
	public ByteBuffer readPage(int pageId) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(Page.pageSize);
		// Creates: [capacity: PAGE_SIZE, position: 0, limit: PAGE_SIZE]
		
		// The start position of given page by pageId. 
		long pos = (long) pageId * Page.pageSize; // since, every page has fixed size (4KB)
		
		channel.read(buf, pos); // so, we are reading on channel by passing two things: buffer (which we created) and pos (which we calculated) 
		
		buf.flip(); // to prepare the buffer for reading. Sets limit to current position, resets position to 0
		
		return buf;
	}
	
	// WRITING PAGE
	public void writePage(int pageId, ByteBuffer src) throws IOException {
		
		// this resets the starting buffer pointer to 0 to check every page
		src.rewind();
		
		long pos = (long) pageId * Page.pageSize; // calculates the position for which page we have to write.
		
		channel.write(src, pos); // Writes the buffer contents to that position in the file
	}
	
	// ALLOCATING A NEW PAGE
	public int allocateNewPage() throws IOException {
		
		// New pageId for a new page according to already present pages in channel.
		// Allocating new pageId is simple because we have fixed PAGE_SIZE (4KB)
		int pageId = (int) (channel.size() / Page.pageSize);
		
		ByteBuffer zero = ByteBuffer.allocate(Page.pageSize); // This creates zero-filled buffer i.e. empty but has all zeroes
		writePage(pageId, zero); // this function will write zero-filled buffer content into that pageId.
		
		force();
		
		return pageId;
	}
	
	// This method: Forces any buffered writes to be written to the physical disk
	// Significance - to ensure the data is written to disk immediately
	public void force() throws IOException {
		channel.force(true);
	}
	
	
	@Override
	public void close() throws IOException {
		channel.close();
	}
	
}
