package db.components;

import db.data.Table;

public class Cursor {

    private Table table;
    private Integer currentKey;
    private boolean endOfTable;  // Indicates if the cursor is one past the last element.

    public Cursor(Table table, Integer currentKey, boolean endOfTable) {
        this.table = table;
        this.currentKey = currentKey;
        this.endOfTable = endOfTable;
    }

    // move the cursor
    public void advance() {
        if (currentKey == null || endOfTable) {
            endOfTable = true;
            return;
        }

        Integer nextKey = table.getBTree().getNextKey(currentKey);

        if (nextKey == null) 
        { endOfTable = true; } 
        
        else 
        { currentKey = nextKey; }
    }

    public Table getTable() 
    { return table;}

    public Integer getCurrentKey() 
    { return currentKey;}

    public boolean isEndOfTable() 
    { return endOfTable;}
}