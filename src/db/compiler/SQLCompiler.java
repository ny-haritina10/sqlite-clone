package db.compiler;

import db.components.InputBuffer;
import db.components.Statement;
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
            return PrepareResult.SUCCESS;
        }

        if (input.equals("select") || input.equals("SELECT")) {
            statement.setType(StatementType.SELECT);
            return PrepareResult.SUCCESS;
        }

        return PrepareResult.UNRECOGNIZED_STATEMENT;
    }
}