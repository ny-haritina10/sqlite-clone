package db.components;

import java.util.Scanner;

public class InputBuffer {

    private String buffer; 
    private Scanner input;

    // read user input line
    public void readInput() 
        throws Exception
    {
        input = new Scanner(System.in);     
        setBuffer(input.nextLine());
    }

    public void close() 
    { input.close(); }

    public String getBuffer() 
    { return buffer; }

    public void setBuffer(String buffer)
        throws Exception 
    {
        if (buffer == null || buffer.length() == 0)
        { throw new Exception("Command Exception: buffer can't be empty !"); }

        this.buffer = buffer;
    }
}
