package db.components;

import db.data.Table;

public class Cursor {

    private Table table;
    private int rowNum;
    private boolean endOfTable;  // Indicates if the cursor is one past the last element.

    public Cursor(Table table, int rowNum, boolean endOfTable) {
        this.table = table;
        this.rowNum = rowNum;
        this.endOfTable = endOfTable;
    }

    // move the cursor
    public void advance() {
        this.rowNum++;
        if (this.rowNum >= table.getNumRows()) 
        { this.endOfTable = true; }
    }

    public Table getTable() 
    { return table;}

    public int getRowNum() 
    { return rowNum;}

    public boolean isEndOfTable() 
    { return endOfTable;}
}