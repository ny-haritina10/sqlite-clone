package db.core;

import db.components.Statement;
import db.data.Row;
import db.data.Table;

/*
 * Will execute the statements
 */
public class VirtualMachine {

    public static boolean executeInsert(Statement statement, Table table) {
        if (table.getNumRows() >= Table.TABLE_MAX_ROWS) {
            System.out.println("Error: Table full.");
            return false;
        }

        // Serialize the row to the correct page and byte offset
        int pageNum = table.getNumRows() / Table.ROWS_PER_PAGE;
        int byteOffset = table.getByteOffset(table.getNumRows());
        byte[] page = table.getPage(pageNum);

        Row.serializeRow(statement.getRowToInsert(), page, byteOffset);
        table.setNumRows(table.getNumRows() + 1);

        return true;
    }

    public static void executeSelect(Table table) {
        for (int i = 0; i < table.numRows; i++) {
            int pageNum = i / Table.ROWS_PER_PAGE;
            int byteOffset = table.getByteOffset(i);
            byte[] page = table.getPage(pageNum);
    
            Row row = Row.deserializeRow(page, byteOffset);
            row.printRow();
        }
    }    


    public void executeStatement(Statement statement, Table table) {
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