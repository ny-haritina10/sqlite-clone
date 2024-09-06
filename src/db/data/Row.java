package db.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*
 * Fixed row for the hard coded table 
 */

public class Row {

    public int id;
    public String username;
    public String email;

    public Row(int id, String username, String email) {
        this.id = id;
        this.username = username;
        this.email = email;
    }

    public static void serializeRow(Row row, byte[] page, int byteOffset) {
        // Write the row data into the page
        ByteBuffer buffer = ByteBuffer.wrap(page, byteOffset, Table.ROW_SIZE);
        buffer.putInt(row.id);

        byte[] usernameBytes = row.username.getBytes(StandardCharsets.UTF_8);
        byte[] emailBytes = row.email.getBytes(StandardCharsets.UTF_8);

        // Write username and email with padding
        buffer.put(Arrays.copyOf(usernameBytes, Table.COLUMN_USERNAME_SIZE));
        buffer.put(Arrays.copyOf(emailBytes, Table.COLUMN_EMAIL_SIZE));
    }

    public static Row deserializeRow(byte[] page, int byteOffset) {
        ByteBuffer buffer = ByteBuffer.wrap(page, byteOffset, Table.ROW_SIZE);
        
        int id = buffer.getInt();
        
        byte[] usernameBytes = new byte[Table.COLUMN_USERNAME_SIZE];
        buffer.get(usernameBytes);
        String username = new String(usernameBytes, StandardCharsets.UTF_8).trim();
        
        byte[] emailBytes = new byte[Table.COLUMN_EMAIL_SIZE];
        buffer.get(emailBytes);
        String email = new String(emailBytes, StandardCharsets.UTF_8).trim();
    
        return new Row(id, username, email);
    }
    
    public void printRow() 
    { System.out.printf("(%d, %s, %s)\n", id, username, email); }
}