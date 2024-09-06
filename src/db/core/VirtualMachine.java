package db.core;

import db.components.Statement;

/*
 * Will execute the statements
 */
public class VirtualMachine {

    public void executeStatement(Statement statement) {
        switch (statement.getType()) {
            case INSERT:
                System.out.println("This is where we would do an insert.");
                break;
            case SELECT:
                System.out.println("This is where we would do a select.");
                break;
        }
        System.out.println("Executed.");
    }
}