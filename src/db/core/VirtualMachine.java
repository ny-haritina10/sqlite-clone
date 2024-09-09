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
        Row rowToInsert = statement.getRowToInsert();
        table.insert(rowToInsert);
        
        // Flush changes to disk
        table.getPager().flush(0, Table.PAGE_SIZE); // needs to be adjusted for B-Tree structure
        
        return true;
    }

    public static void executeSelect(Table table) 
        throws IOException
    {
        Cursor cursor = table.start();
        while (!cursor.isEndOfTable()) {
            ByteBuffer rowBuffer = table.cursorValue(cursor);
            if (rowBuffer != null) {
                Row row = Row.deserializeRow(rowBuffer, 0);
                row.printRow();
            }
            cursor.advance();
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