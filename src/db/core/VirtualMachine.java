package db.core;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.components.Statement;
import db.data.Row;
import db.data.Table;

/*
 * Will execute the statements
 */

public class VirtualMachine {

    public static boolean executeInsert(Statement statement, Table table) 
        throws IOException
    {
        if (table.getNumRows() >= Table.TABLE_MAX_ROWS) {
            System.out.println("Error: Table full.");
            return false;
        }

        /// Serialize the row to the correct page and byte offset
        int rowNum = table.getNumRows();
        int pageNum = rowNum / Table.ROWS_PER_PAGE;
        int byteOffset = table.getByteOffset(rowNum);

        ByteBuffer page = table.getPager().getPage(pageNum);
        Row.serializeRow(statement.getRowToInsert(), page, byteOffset);

        // Update the number of rows in the table
        table.setNumRows(table.getNumRows() + 1);

        // Flush the page to disk to persist changes
        try 
        { table.getPager().flush(pageNum, Table.PAGE_SIZE); } 
        
        catch (Exception e) {
            System.err.println("Error while flushing page to disk: " + e.getMessage());
            return false;
        }

        return true;
    }

    public static void executeSelect(Table table) 
        throws IOException
    {
        for (int i = 0; i < table.getNumRows(); i++) {
            int pageNum = i / Table.ROWS_PER_PAGE;
            int byteOffset = table.getByteOffset(i);

            ByteBuffer page = table.getPager().getPage(pageNum);
            Row row = Row.deserializeRow(page, byteOffset);

            if (row.id != 0)        // Assuming 0 is an invalid ID for your data
            { row.printRow(); }        
        }
    }  


    public void executeStatement(Statement statement, Table table) 
        throws IOException
    {
        switch (statement.getType()) {
            case INSERT:
                if (executeInsert(statement, table)) {
                    System.out.println("Executed.");
                }
                break;

            case SELECT:
                executeSelect(table);
                System.out.println("Executed.");
                break;
        }
    }
}