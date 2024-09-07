package db.backend;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

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

    public Pager(String filename) throws IOException {
        this.file = new RandomAccessFile(filename, "rw");   // creating the file with read/write mode
        this.fileLength = file.length();
        this.pages = new ByteBuffer[Table.TABLE_MAX_PAGES];
    }

    public ByteBuffer getPage(int pageNum) 
        throws IOException 
    {
        if (pageNum > Table.TABLE_MAX_PAGES) 
        { throw new IllegalArgumentException("Page number out of bounds."); }

        if (pages[pageNum] == null) {
            ByteBuffer page = ByteBuffer.allocate(Table.PAGE_SIZE);
            
            if (pageNum < fileLength / Table.PAGE_SIZE) {
                file.seek(pageNum * Table.PAGE_SIZE);
                file.read(page.array());
            }

            pages[pageNum] = page;
        }

        return pages[pageNum];
    }

    // flush all the cached pages to disk to persist all changes (vider)
    public void flush(int pageNum, int size) 
        throws IOException 
    {
        if (pages[pageNum] == null) 
        { throw new IllegalStateException("Trying to flush a null page."); }

        file.seek(pageNum * Table.PAGE_SIZE);
        file.write(pages[pageNum].array(), 0, size);
    }

    public void close() 
        throws IOException 
    { file.close(); }

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