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

    public static boolean executeInsert(Statement statement, Table table) 
        throws IOException
    {
        Row rowToInsert = statement.getRowToInsert();
        BTree<Integer, Row> bTree = table.getBTree(); // Assume Table now has a BTree

        // Insert into B-tree
        bTree.insert(rowToInsert.getId(), rowToInsert);
        
        // Save B-tree to disk
        bTree.saveToDisk();
        
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