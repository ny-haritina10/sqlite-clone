package main;

import db.core.REPL;

public class Main {
    public static void main(String[] args) {
        try {
            REPL console = new REPL("_file.dat");
            console.run();    
        } 
        
        catch (Exception e) 
        { e.printStackTrace(); }
    }   
}