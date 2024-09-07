package db.data;

import java.io.IOException;

public class Database {
    
    private Table table;

    public void open(String filename) 
        throws IOException 
    { this.table = new Table(filename); }

    public void close() 
        throws IOException 
    { table.close(); }

    public Table getTable() 
    { return table; }

    public void setTable(Table table) 
    { this.table = table; }   
}