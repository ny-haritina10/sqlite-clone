package db.compiler;

import db.components.InputBuffer;
import db.components.Statement;
import db.data.Row;
import db.enums.PrepareResult;
import db.enums.StatementType;

/*
 * Responsible for parsing SQL input into Statement
 */

public class SQLCompiler {
    
    public PrepareResult prepareStatement(InputBuffer inputBuffer, Statement statement) {
        String input = inputBuffer.getBuffer();

        if (input.startsWith("insert") || input.startsWith("INSERT")) {
            statement.setType(StatementType.INSERT);
            
            // Parse the input string to extract the id, username, and email
            String[] tokens = input.split(" ");
            if (tokens.length < 4) {
                System.out.println("Syntax error: expected 'insert <id> <username> <email>'");
                return PrepareResult.UNRECOGNIZED_STATEMENT;
            }

            try {
                int id = Integer.parseInt(tokens[1]);
                String username = tokens[2];
                String email = tokens[3];

                // Create a new Row object and set it to the statement
                Row rowToInsert = new Row(id, username, email);
                statement.setRowToInsert(rowToInsert);
            } 
            
            catch (NumberFormatException e) {
                System.out.println("Error: ID must be an integer.");
                return PrepareResult.UNRECOGNIZED_STATEMENT;
            }

            return PrepareResult.SUCCESS;
        }

        if (input.equals("select") || input.equals("SELECT")) {
            statement.setType(StatementType.SELECT);
            return PrepareResult.SUCCESS;
        }

        return PrepareResult.UNRECOGNIZED_STATEMENT;
    }
}