package db.data;

import java.io.IOException;

public class Database {
    
    private Table table;

    public void open(String fileName) throws IOException {
        try {
            table = new Table(fileName);
        } catch (IOException e) {
            System.err.println("Failed to open database: " + e.getMessage());
            throw e;
        }
    }

    public void close() 
        throws IOException 
    { table.close(); }

    public Table getTable() 
    { return table; }

    public void setTable(Table table) 
    { this.table = table; }   
}