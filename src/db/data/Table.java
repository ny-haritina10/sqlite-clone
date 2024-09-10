package db.data;

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
        throws Exception 
    {
        this.pager = new Pager(fileName);
        this.bTree = new BTree<>(B_TREE_ORDER, this.pager);

        if (pager.getFileLength() > 0) {
            // Load existing B-tree
            ByteBuffer metaPage = pager.getPage(0);
            int savedOrder = metaPage.getInt(4);
            if (savedOrder <= 0) {
                savedOrder = B_TREE_ORDER; // Use default if saved order is invalid
                System.out.println("Warning: Invalid saved order. Using default: " + B_TREE_ORDER);
            }
            this.bTree = new BTree<>(savedOrder, this.pager);
            this.bTree.loadFromDisk();
        } 
        
        else {
            // Initialize new B-tree
            this.bTree = new BTree<>(B_TREE_ORDER, this.pager);
            ByteBuffer metaPage = pager.getPage(0);
            metaPage.putInt(0, -1);  // No root node yet
            metaPage.putInt(4, B_TREE_ORDER);
            pager.flush(0, 8);
        }
    }

    public ByteBuffer cursorValue(Cursor cursor) 
        throws Exception 
    {
        Row row = bTree.search(cursor.getCurrentKey());

        if (row == null) 
        { return null; }

        ByteBuffer buffer = ByteBuffer.allocate(ROW_SIZE);
        row.serializeRow(buffer);
        
        buffer.flip();

        return buffer;
    }
    
    public void close() 
        throws Exception 
    {
        if (bTree != null) 
        { bTree.saveToDisk(); }

        if (pager != null) 
        { pager.close(); }
    }

    // get a Cursor at the start of the current Table
    public Cursor start() 
    {
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