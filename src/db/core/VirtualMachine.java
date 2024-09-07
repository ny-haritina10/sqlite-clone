package db.core;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.components.Cursor;
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

        Row rowToInsert = statement.getRowToInsert();
        Cursor cursor = table.end();       // Get a cursor at the end of the table

        ByteBuffer page = table.cursorValue(cursor);
        Row.serializeRow(rowToInsert, page, page.position());

        // Update the number of rows in the table
        table.setNumRows(table.getNumRows() + 1);

        // Flush the page to disk to persist changes
        try 
        { table.getPager().flush((table.getNumRows()) / Table.ROWS_PER_PAGE, Table.PAGE_SIZE); } 
        
        catch (Exception e) {
            System.err.println("Error while flushing page to disk: " + e.getMessage());
            return false;
        }

        return true;
    }

    public static void executeSelect(Table table) 
        throws IOException
    {
        Cursor cursor = table.start();      
        while (!cursor.isEndOfTable()) {
            ByteBuffer page = table.cursorValue(cursor);
            Row row = Row.deserializeRow(page, page.position());

            if (row.id != 0)        // Assuming 0 is an invalid ID for your data
            { row.printRow(); }     
            
            cursor.advance();  // Move to the next row
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