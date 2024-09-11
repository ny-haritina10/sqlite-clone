package db.core;

import java.io.IOException;

import db.compiler.MetaCommandHandler;
import db.compiler.SQLCompiler;
import db.components.InputBuffer;
import db.components.Statement;
import db.data.Database;

/*
 * REPL : Read - Execute - Print - Loop
 * Infinite loop 
 * Print the prompt 
 * Gets the line of input
 * Process that line 
 */

public class REPL {

    private InputBuffer inputBuffer;
    private MetaCommandHandler metaCommandHandler;
    private SQLCompiler sqlCompiler;
    private VirtualMachine virtualMachine;
    private Database database;

    private String fileName;

    public REPL(String fileName) {
        inputBuffer = new InputBuffer();
        metaCommandHandler = new MetaCommandHandler();
        sqlCompiler = new SQLCompiler();
        virtualMachine = new VirtualMachine();
        database = new Database();

        this.fileName = "D:\\Studies\\ITU\\S5\\INF - Side Project\\sqlite-clone\\datafile\\" + fileName;
    }

    public void run() {
        try {

            database.open(fileName);

            while (true) {
                prompt();
                inputBuffer.readInput();        // read user input

                // handle meta command
                if (inputBuffer.getBuffer().startsWith(".")) {
                    switch (metaCommandHandler.handleMetaCommand(inputBuffer)) {
                        case SUCCESS:
                            continue;
                        case UNRECOGNIZED_COMMAND:
                            System.out.println("Unrecognized command '" + inputBuffer.getBuffer() + "'");
                            continue;
                    }
                }

                // handle statement
                Statement statement = new Statement();
                switch (sqlCompiler.prepareStatement(inputBuffer, statement)) {
                    case SUCCESS:
                        break;
                    case UNRECOGNIZED_STATEMENT:
                        System.out.println("Unrecognized statement keyword at start of '" + inputBuffer.getBuffer() + "'");
                        continue;
                }

                // execute statement
                virtualMachine.executeStatement(statement, database.getTable());
            }
        } 
        
        catch (Exception e) {
            e.printStackTrace();
        } 
        
        finally {
            try 
            { database.close(); } 
            
            catch (IOException e) {
                System.err.println("Error closing the table: " + e.getMessage());
            }
        }
    }

    private void prompt() 
    { System.out.print("$sql-lite-clone > "); }
}