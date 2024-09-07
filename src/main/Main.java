package main;

import db.core.REPL;

public class Main {
    public static void main(String[] args) {
        try {
            REPL console = new REPL("D:\\Studies\\ITU\\S5\\INF - Side Project\\sqlite-clone\\_file.dat");
            console.run();    
        } 
        
        catch (Exception e) 
        { e.printStackTrace(); }
    }   
}