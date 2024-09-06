package db.data;

import java.util.ArrayList;
import java.util.List;

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

    // size of each Page
    // memory's block that holds multiple rows
    public static final int PAGE_SIZE = 4096;   

    public static final int COLUMN_USERNAME_SIZE = 32;     
    public static final int COLUMN_EMAIL_SIZE = 255;

    // total size in bytes of a Row
    // sum of username size + eamil size + integr bytes
    public static final int ROW_SIZE = Integer.BYTES + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;

    // determines how many rows can fit into a single page
    // e.g : ROWS_PER_PAGE = 4096 / 291 ≈ 14 rows per page.
    public static final int ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;

    // define a max page number per tables
    public static final int TABLE_MAX_PAGES = 100;

    // define a max rows number per table 
    public static final int TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

    // list that holds the data for each pages 
    // each page is represented by an array of byte
    public List<byte[]> pages;

    // keeps track of the total number of rows currently stored in the table
    public int numRows;

    public Table() {
        pages = new ArrayList<>(TABLE_MAX_PAGES);       // arrayList define with an initial capacity
        numRows = 0;
    }

    public byte[] getPage(int pageNum) {

        if (pageNum >= pages.size()) {      // requested page doesn't exist

            // then, create a new page , add it to the list and return this new page
            byte[] newPage = new byte[PAGE_SIZE];
            pages.add(newPage);

            // ensures that pages are only created when needed, saving memory
            return newPage;
        }

        return pages.get(pageNum);
    }

    public int getByteOffset(int rowNum) 
    { return (rowNum % ROWS_PER_PAGE) * ROW_SIZE; }

    public List<byte[]> getPages() 
    { return pages; }

    public void setPages(List<byte[]> pages) 
    { this.pages = pages; }

    public int getNumRows() 
    { return numRows; }

    public void setNumRows(int numRows) 
    { this.numRows = numRows; }
}