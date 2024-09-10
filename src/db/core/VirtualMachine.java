package db.core;

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
        throws Exception
    {
        Row rowToInsert = statement.getRowToInsert();
        BTree<Integer, Row> bTree = table.getBTree(); 

        bTree.insert(rowToInsert.getId(), rowToInsert);                
        return true;
    }

    public static void executeSelect(Table table) 
        throws Exception
    {
        Cursor cursor = table.start();

        if (cursor.getCurrentKey() == null)
        { throw new Exception("Table is empty !"); }

        while (!cursor.isEndOfTable()) {
            ByteBuffer rowBuffer = table.cursorValue(cursor);
            if (rowBuffer != null) {
                Row row = Row.deserializeRow(rowBuffer);
                row.printRow();
            }
            
            cursor.advance();
        }
    }  

    public void executeStatement(Statement statement, Table table) 
        throws Exception
    {
        switch (statement.getType()) {
            case INSERT:
                if (executeInsert(statement, table)) {
                    System.out.println("Insertion executed !");
                }
                break;

            case SELECT:
                executeSelect(table);
                System.out.println("Selection executed !");
                break;
        }
    }
}