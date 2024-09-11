package db.core;

import java.io.IOException;
import java.nio.ByteBuffer;

import db.components.Cursor;
import db.components.Statement;
import db.data.Row;
import db.data.Table;
import db.datastructure.BTree;

/*
 * Used to execute Statement
 */

public class VirtualMachine {

    public static boolean executeInsert(Statement statement, Table table) throws IOException {
        System.out.println("Executing INSERT operation");
        Row rowToInsert = statement.getRowToInsert();
        System.out.println("Row to insert: " + rowToInsert);
        BTree<Integer, Row> bTree = table.getBTree();
    
        // Insert into B-tree
        bTree.insert(rowToInsert.getId(), rowToInsert);
        System.out.println("Row inserted into B-Tree");
        
        // Save B-tree to disk
        table.saveBTreeToDisk();
        bTree.saveToDisk();
        
        System.out.println("B-Tree saved to disk");
        
        return true;
    }

    public static void executeSelect(Table table) throws IOException {
        System.out.println("Executing SELECT operation");
        Cursor cursor = table.start();
        if (cursor == null) {
            System.out.println("Cursor is null, aborting SELECT operation");
            return;
        }
        System.out.println("Cursor created, current key: " + cursor.getCurrentKey());
    
        while (cursor != null && !cursor.isEndOfTable()) {
            ByteBuffer rowBuffer = table.cursorValue(cursor);
            if (rowBuffer != null) {
                Row row = Row.deserializeRow(rowBuffer, 0);
                System.out.println("Retrieved row: " + row);
                row.printRow();
            } else {
                System.out.println("Row buffer is NULL for key: " + cursor.getCurrentKey());
            }
    
            cursor.advance();
            System.out.println("Advanced cursor. New key: " + cursor.getCurrentKey());
        }
        
        System.out.println("SELECT operation completed");
    }

    public void executeStatement(Statement statement, Table table) 
        throws IOException
    {
        switch (statement.getType()) {
            case INSERT:
                if (executeInsert(statement, table)) {
                    System.out.println("Insertion executed.");
                }
                break;

            case SELECT:
                executeSelect(table);
                System.out.println("Selection executed.");
                break;
        }
    }
}