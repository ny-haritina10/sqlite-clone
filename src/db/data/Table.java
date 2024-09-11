package db.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.backend.Pager;
import db.components.Cursor;
import db.datastructure.BTree;
import db.datastructure.BTreeNode;

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
        this.bTree = new BTree<>(B_TREE_ORDER, this.pager);

        loadBTreeFromDisk();    
    }

    
    private void loadBTreeFromDisk() throws IOException {
        System.out.println("Loading B-Tree from disk");
        ByteBuffer metaPage = pager.getPage(0);
        int rootPageNum = metaPage.getInt(0);
        int order = metaPage.getInt(4);
        
        System.out.println("Meta page content: rootPageNum=" + rootPageNum + ", order=" + order);
        
        // Ensure order is valid
        if (order <= 0) {
            order = B_TREE_ORDER; // Use default order if the stored value is invalid
            System.out.println("Invalid order, using default: " + order);
        }
        
        this.bTree = new BTree<>(order, this.pager);
        if (rootPageNum > 0) {
            System.out.println("Loading root node from page " + rootPageNum);
            BTreeNode<Integer, Row> root = loadNodeFromDisk(rootPageNum);
            this.bTree.setRoot(root);
            System.out.println("Root node loaded. Size: " + root.getSize() + ", Is leaf: " + root.isLeaf());
        } else {
            System.out.println("No root node found (rootPageNum <= 0), creating new root");
            this.bTree.createNewRoot();
        }
        System.out.println("B-Tree loading complete");
    }
    
    private BTreeNode<Integer, Row> loadNodeFromDisk(int pageNum) throws IOException {
        System.out.println("Loading node from page " + pageNum);
        ByteBuffer nodePage = pager.getPage(pageNum);
        BTreeNode<Integer, Row> node = this.bTree.deserializeNode(nodePage);
        
        System.out.println("Node deserialized. Size: " + node.getSize() + ", Is leaf: " + node.isLeaf());
        
        if (!node.isLeaf()) {
            int childrenOffset = 5 + (node.getSize() * (Table.ROW_SIZE + Integer.BYTES));
            for (int i = 0; i <= node.getSize(); i++) {
                int childPageNum = nodePage.getInt(childrenOffset + (i * Integer.BYTES));
                System.out.println("Child " + i + " page number: " + childPageNum);
                if (childPageNum != -1) {
                    BTreeNode<Integer, Row> childNode = loadNodeFromDisk(childPageNum);
                    node.insertChild(i, childNode);
                    System.out.println("Inserted child " + i + ". Size: " + childNode.getSize() + ", Is leaf: " + childNode.isLeaf());
                }
            }
        }
        
        return node;
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
        System.out.println("Closing table and saving B-Tree to disk");
        saveBTreeToDisk();
        bTree.saveToDisk();
        pager.close();
        System.out.println("Table closed and B-Tree saved");
    }

    public void saveBTreeToDisk() 
        throws IOException 
    { bTree.saveToDisk(); }

    // get a Cursor at the start of the current Table
    public Cursor start() {
        System.out.println("Starting cursor creation");
        Integer minKey = bTree.getMinKey();
        System.out.println("Minimum key in B-Tree: " + minKey);
        if (minKey == null) {
            System.out.println("B-Tree is empty, returning end-of-table cursor");
            return new Cursor(this, null, true); // Empty tree 
        }
        System.out.println("Returning cursor with minimum key: " + minKey);
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