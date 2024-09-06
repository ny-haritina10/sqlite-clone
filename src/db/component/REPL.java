package db.component;

import java.util.Scanner;

/*
 * REPL : Read - Execute - Print - Loop
 * Infinite loop 
 * Print the prompt 
 * Gets the line of input
 * Process that line 
 */

public class REPL {

    /*
     * executed function to run the console
     */
    public void run() {
        String command = new String();
        Scanner input = new Scanner(System.in);     // user input line
        System.out.print("\n");

        while (true) {
            System.out.print("$sqlite-clone> ");
            command = input.nextLine();

            if (command.equalsIgnoreCase("exit")) {
                System.out.println("Good Bye!"); 
                break; 
            }
        }

        input.close();
    }
}