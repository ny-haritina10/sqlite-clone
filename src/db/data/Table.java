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

    private static final int METADATA_PAGE = 0;


    public static final int B_TREE_ORDER = 4;       // default b-tree order

    private BTree<Integer, Row> bTree;
    private Pager pager;    

    public Table(String fileName) throws Exception {
        this.pager = new Pager(fileName);
        loadMetadata();

        if (this.bTree == null) {
            this.bTree = new BTree<>(B_TREE_ORDER, this.pager);
        }
    }

     private void loadMetadata() throws Exception {
        ByteBuffer metadataBuffer = pager.getPage(METADATA_PAGE);
        if (metadataBuffer.getInt(0) != 0) { // Check if rootPageNum is not 0
            int rootPageNum = metadataBuffer.getInt(0);
            int numNodes = metadataBuffer.getInt(Integer.BYTES);
            this.bTree = new BTree<>(B_TREE_ORDER, this.pager, rootPageNum, numNodes);
        }
    }

    private void saveMetadata() throws Exception {
        ByteBuffer metadataBuffer = pager.getPage(METADATA_PAGE);
        metadataBuffer.putLong(0, bTree.getRootPageNum());
        metadataBuffer.putInt(Integer.BYTES, bTree.getNumNodes());
        pager.writePage(METADATA_PAGE);
    }

    public void insert(Row row) throws Exception {
        bTree.insert(row.id, row);
        saveMetadata();
    }

    public Row search(int id) throws Exception {
        return bTree.search(id);
    }

    public void delete(int id) throws Exception {
        bTree.delete(id);
        saveMetadata();
    }

    public void close() throws Exception {
        if (pager != null) {
            saveMetadata();
            pager.close();
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

    // get a Cursor at the start of the current Table
    public Cursor start() 
        throws Exception
    {
        Integer minKey = bTree.getMinKey();
        if (minKey == null) 
        { return new Cursor(this, null, true); } // Empty tree 
        
        return new Cursor(this, minKey, false);
    }

    // get a Cursor at the end of the cuurent Table
    public Cursor end() 
        throws Exception
    {
        Integer maxKey = bTree.getMaxKey();
        if (maxKey == null) 
        { return new Cursor(this, null, true); } // Empty tree 
        
        return new Cursor(this, maxKey, true);
    }

    public int getByteOffset(int rowNum) 
    { return (rowNum % ROWS_PER_PAGE) * ROW_SIZE; }

    public Pager getPager() 
    { return pager; }

    public BTree<Integer, Row> getBTree() 
    { return bTree; }
}