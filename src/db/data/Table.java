package db.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.backend.Pager;
import db.components.Cursor;
import db.datastructure.BTree;

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

    public static final int B_TREE_ORDER = 4;       // default b-tree order

    private BTree<Integer, Row> bTree;
    private Pager pager;    

    public Table(String fileName)
        throws IOException 
    {
        this.pager = new Pager(fileName);
        this.bTree = new BTree<>(B_TREE_ORDER);
        loadBTreeFromDisk();    
    }

    private void loadBTreeFromDisk() throws IOException {
        long numPages = pager.getFileLength() / PAGE_SIZE;

        for (int i = 0; i < numPages; i++) {
            ByteBuffer page = pager.getPage(i);
            while (page.hasRemaining()) {
                Row row = Row.deserializeRow(page, page.position());
                bTree.insert(row.id, row);
            }
        }
    }

    public ByteBuffer cursorValue(Cursor cursor) 
        throws IOException 
    {
        Row row = bTree.search(cursor.getCurrentKey());

        if (row == null) 
        { return null; }

        ByteBuffer buffer = ByteBuffer.allocate(ROW_SIZE);
        Row.serializeRow(row, buffer, 0);
        buffer.flip();

        return buffer;
    }
    
    public void close() throws IOException {
        saveBTreeToDisk();
        pager.close();
    }

    private void saveBTreeToDisk() 
        throws IOException 
    {
        // Implement a method to save the B-Tree structure to disk
        // This could involve serializing the B-Tree nodes and writing them to pages
    }

    // get a Cursor at the start of the current Table
    public Cursor start() {
        Integer minKey = bTree.getMinKey();
        if (minKey == null) 
        { return new Cursor(this, null, true); } // Empty tree 
        
        return new Cursor(this, minKey, false);
    }

    // get a Cursor at the end of the cuurent Table
    public Cursor end() {
        Integer maxKey = bTree.getMaxKey();
        if (maxKey == null) 
        { return new Cursor(this, null, true); } // Empty tree 
        
        return new Cursor(this, maxKey, true);
    }

    public int getByteOffset(int rowNum) 
    { return (rowNum % ROWS_PER_PAGE) * ROW_SIZE; }

    public void insert(Row row) 
    { bTree.insert(row.id, row); }

    public Row search(int id) 
    { return bTree.search(id); }

    public void delete(int id) 
    { bTree.delete(id); }

    public Pager getPager() 
    { return pager; }

    public BTree<Integer, Row> getBTree() 
    { return bTree; }
}