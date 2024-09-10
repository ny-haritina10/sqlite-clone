package db.data;


public class Database {
    
    private Table table;

    public void open(String fileName) throws Exception {
        try {
            this.table = new Table(fileName);
        } catch (Exception e) {
            System.err.println("Error opening database: " + e.getMessage());
            throw e;
        }
    }

    public void close() throws Exception {
        if (this.table != null) {
            try {
                this.table.close();
            } catch (Exception e) {
                System.err.println("Error closing database: " + e.getMessage());
                throw e;
            } finally {
                this.table = null;
            }
        }
    }

    public Table getTable() 
    { return table; }

    public void setTable(Table table) 
    { this.table = table; }   
}