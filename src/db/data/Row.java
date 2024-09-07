package db.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {

    public int id;
    public String username;
    public String email;

    public Row(int id, String username, String email) {
        this.id = id;
        this.username = username;
        this.email = email;
    }

    // Serialize the row into the ByteBuffer page at the given byteOffset
    public static void serializeRow(Row row, ByteBuffer page, int byteOffset) {
        // Set the position of the buffer to the byteOffset
        page.position(byteOffset);

        // Serialize id, username, and email
        page.putInt(row.id);

        byte[] usernameBytes = row.username.getBytes(StandardCharsets.UTF_8);
        byte[] emailBytes = row.email.getBytes(StandardCharsets.UTF_8);

        // Write username and email with padding
        page.put(Arrays.copyOf(usernameBytes, Table.COLUMN_USERNAME_SIZE));
        page.put(Arrays.copyOf(emailBytes, Table.COLUMN_EMAIL_SIZE));
    }

    // Deserialize a row from the ByteBuffer page at the given byteOffset
    public static Row deserializeRow(ByteBuffer page, int byteOffset) {
        // Set the position of the buffer to the byteOffset
        page.position(byteOffset);

        int id = page.getInt();

        byte[] usernameBytes = new byte[Table.COLUMN_USERNAME_SIZE];
        page.get(usernameBytes);
        String username = new String(usernameBytes, StandardCharsets.UTF_8).trim();

        byte[] emailBytes = new byte[Table.COLUMN_EMAIL_SIZE];
        page.get(emailBytes);
        String email = new String(emailBytes, StandardCharsets.UTF_8).trim();

        return new Row(id, username, email);
    }

    public void printRow() 
    { System.out.printf("(%d, %s, %s)\n", id, username, email); }
}