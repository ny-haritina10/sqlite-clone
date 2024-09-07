package db.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.backend.Pager;
import db.components.Cursor;

/*
 * Handle the storage adn organization of rows
 * Ensure that rows are stored compactly in pages (efficient way to manage data)
 * The Table stores rows in a series of fixed-size pages.
 * Each page can hold multiple rows, and new pages are created as needed when the table grows.
 * Rows are stored as byte arrays
 * the Table class simulates a simple, append-only storage mechanism, 
 * where rows are written sequentially into fixed-size pages
 */

public class Table {

    public static final int PAGE_SIZE = 4096;   
    public static final int COLUMN_USERNAME_SIZE = 32;     
    public static final int COLUMN_EMAIL_SIZE = 255;
    public static final int ROW_SIZE = Integer.BYTES + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;
    public static final int ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
    public static final int TABLE_MAX_PAGES = 100;
    public static final int TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

    private int numRows;
    private Pager pager;

    public Table(String fileName)
        throws IOException 
    {
        this.pager = new Pager(fileName);
        this.numRows = (int) pager.getFileLength() / ROW_SIZE;
    }

    // get the exact location of a row in a page
    // locate a specific row using a Cursor
    public ByteBuffer cursorValue(Cursor cursor) 
        throws IOException 
    {
        int pageNum = cursor.getRowNum() / Table.ROWS_PER_PAGE;
        ByteBuffer page = getPager().getPage(pageNum);
        
        int rowOffset = cursor.getRowNum() % Table.ROWS_PER_PAGE;
        int byteOffset = rowOffset * ROW_SIZE;
        
        return (ByteBuffer) page.position(byteOffset);
    }
    

    public void close() 
        throws IOException 
    {
        int fullPages = numRows / ROWS_PER_PAGE;

        for (int i = 0; i < fullPages; i++) {
            if (pager.getPage(i) != null) 
            { pager.flush(i, Table.PAGE_SIZE); }    // save data by flushing all pages
        }

        pager.close();
    }

    // get a Cursor at the start of the current Table
    public Cursor start() 
    { return new Cursor(this, 0, getNumRows() == 0); }

    // get a Cursor at the end of the cuurent Table
    public Cursor end() 
    { return new Cursor(this, getNumRows(), true); }


    public int getByteOffset(int rowNum) 
    { return (rowNum % ROWS_PER_PAGE) * ROW_SIZE; }

    public int getNumRows() 
    { return numRows; }

    public void setNumRows(int numRows) 
    { this.numRows = numRows; }

    public Pager getPager() 
    { return pager; }

    public void setPager(Pager pager) 
    { this.pager = pager; }
}