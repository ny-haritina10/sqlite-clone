package db.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Row {

    public int id;
    public String username;
    public String email;

    public Row(int id, String username, String email) {
        this.id = id;
        this.username = username;
        this.email = email;
    }

    public void serializeRow(ByteBuffer buffer) {
        buffer.putInt(id);

        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        buffer.put(usernameBytes);
        buffer.put(new byte[Table.COLUMN_USERNAME_SIZE - usernameBytes.length]); // Padding

        byte[] emailBytes = email.getBytes(StandardCharsets.UTF_8);
        buffer.put(emailBytes);
        buffer.put(new byte[Table.COLUMN_EMAIL_SIZE - emailBytes.length]); // Padding
    }

    public static Row deserializeRow(ByteBuffer buffer) {
        // Not enough data in the buffer for a complete row
        if (buffer.remaining() < Table.ROW_SIZE) 
        { return null; }

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

    public int getId() 
    { return id; }

    public void setId(int id) 
    { this.id = id; }

    public String getUsername() 
    { return username; }

    public void setUsername(String username) 
    { this.username = username; }

    public String getEmail() 
    { return email; }

    public void setEmail(String email) 
    { this.email = email; }
}