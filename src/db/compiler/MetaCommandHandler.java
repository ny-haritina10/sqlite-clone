package db.compiler;

import db.components.InputBuffer;
import db.enums.MetaCommandResult;

/*
 * Handle non SQL Statement (meta commands)
 */

public class MetaCommandHandler {

    public MetaCommandResult handleMetaCommand(InputBuffer inputBuffer) {
        String input = inputBuffer.getBuffer();

        if (input.equals(".exit")) {
            System.out.println("Bye, see you soon ^^");
            inputBuffer.close();
            System.exit(0);

            return MetaCommandResult.SUCCESS;
        }

        return MetaCommandResult.UNRECOGNIZED_COMMAND;
    }
}