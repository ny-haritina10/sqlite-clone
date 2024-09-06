package main;

import db.core.REPL;

public class Main {
    public static void main(String[] args) {
        try {
            REPL console = new REPL();
            console.run();    
        } 
        
        catch (Exception e) {
            e.printStackTrace();
        }
    }   
}