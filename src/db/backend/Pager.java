package db.backend;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.BitSet;

import db.data.Table;

/*
 * Manage interaction between the in-memory cache and the disk (where the db is persisted)
 * -> Fetch page from memory or disk
 * -> Write page back to disk
 * -> Track file size and manage memory allocation for new page 
 * 
 * ------------------------------------------------------------
 * 
 * Abstraction layer between database and the file system.
 * Primary Role is to manage reading and writing pages to and from the db file
 * Allow data persistence
 */

public class Pager {
    
    private RandomAccessFile file;
    private long fileLength;
    private ByteBuffer[] pages;
    private BitSet freePages;

    public Pager(String filename) 
        throws Exception 
    {
        this.file = new RandomAccessFile(filename, "rw");
        this.fileLength = file.length();
        this.pages = new ByteBuffer[Table.TABLE_MAX_PAGES];
        this.freePages = new BitSet(Table.TABLE_MAX_PAGES);
        
        initializeFreePages();
    }

    private void initializeFreePages() throws Exception {
        long totalPages = fileLength / Table.PAGE_SIZE;
        for (int i = 0; i < totalPages; i++) {
            freePages.set(i);
        }
    }

    public long getFreePage() throws Exception {
        int freePage = freePages.nextClearBit(0);
        if (freePage >= Table.TABLE_MAX_PAGES) {
            throw new Exception("No free pages available");
        }
        freePages.set(freePage);
        if (freePage >= fileLength / Table.PAGE_SIZE) {
            fileLength = (freePage + 1) * (long) Table.PAGE_SIZE;
        }
        return freePage;
    }

    public void writePage(int pageNum) throws Exception {
        if (pages[pageNum] == null) {
            throw new IllegalStateException("Trying to write a null page.");
        }
        file.seek(pageNum * (long) Table.PAGE_SIZE);
        file.write(pages[pageNum].array());
    }

    public void freePage(int pageNum) {
        if (pageNum < 0 || pageNum >= Table.TABLE_MAX_PAGES) {
            throw new IllegalArgumentException("Invalid page number");
        }
        freePages.clear(pageNum);
        pages[pageNum] = null;
    }

    public ByteBuffer getPage(int pageNum) throws Exception {
        if (pageNum > Table.TABLE_MAX_PAGES) {
            throw new IllegalArgumentException("Page number out of bounds.");
        }

        if (pages[pageNum] == null) {
            ByteBuffer page = ByteBuffer.allocate(Table.PAGE_SIZE);
            
            if (pageNum < fileLength / Table.PAGE_SIZE) {
                file.seek(pageNum * (long) Table.PAGE_SIZE);
                file.read(page.array());
            }

            pages[pageNum] = page;
        }

        return pages[pageNum];
    }

    public void flush(int pageNum, int size) throws Exception {
        if (pages[pageNum] == null) {
            throw new IllegalStateException("Trying to flush a null page.");
        }

        file.seek(pageNum * (long) Table.PAGE_SIZE);
        file.write(pages[pageNum].array(), 0, size);
    }

    public void close() throws Exception {
        file.close();
    }

    public RandomAccessFile getFile() 
    { return file; }

    public void setFile(RandomAccessFile file) 
    { this.file = file; }

    public long getFileLength() 
    { return fileLength; }

    public void setFileLength(long fileLength) 
    { this.fileLength = fileLength; }

    public ByteBuffer[] getPages() 
    { return pages; }

    public void setPages(ByteBuffer[] pages) 
    { this.pages = pages; }
}