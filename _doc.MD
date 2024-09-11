1. Pager
Purpose: 
    Handles file I/O between the in-memory cache (pages) and the database file on disk.
Key Tasks:
    Reads pages from the file if they are not already in memory.
    Writes pages back to disk to persist data.
    Manages file size and page boundaries.


2. Table
Purpose: 
    Manages the structure of the table, including pages and rows.
Key Tasks:
    Calculates where rows should be placed within pages.
    Tracks the total number of rows in the table.
    Uses the Pager to retrieve pages from the database.


3. Cursor
Purpose: 
    Abstracts the position in the table (row-based), making it easier to perform operations on rows (like inserting, selecting, and updating).
Key Tasks:
    Keeps track of the current row and can move through the table.
    Points to the start or end of a table.
    Helps access and modify specific rows through cursor_value().


4. Row
Purpose: 
    Represents a single row in the table with fixed columns (id, username, email).
Key Tasks:
    Serializes and deserializes row data into and from byte arrays for storage in pages.
    Provides methods for printing rows.


5. VirtualMachine
Purpose: 
    Executes SQL statements (insert, select) using table and cursor abstractions.
Key Tasks:
    Uses a cursor to find the correct place in the table for data manipulation.
    Handles logic for iterating through rows and updating them.


6. Database
Purpose: 
    Encapsulates the entire database, initializing the Table and managing its data.


7. SQLCompiler
Purpose: 
    Compiles user SQL input into executable statements.
Key Tasks:
    Translates SQL strings into structured statements (e.g., INSERT, SELECT).
    Identifies whether a statement is valid or unrecognized.


8. MetaCommandHandler
Purpose: 
    Handles special commands that aren't SQL (e.g., .exit).
Key Tasks:
    Processes meta-commands (non-SQL commands).
    Performs operations like quitting the REPL.


9. REPL (Read-Eval-Print Loop)
Purpose: 
    Provides the interactive console where users input commands.
Key Tasks:
    Reads input, processes commands, and delegates to the VirtualMachine for execution.


10. Statement
Purpose: 
    Represents an SQL command (INSERT, SELECT).
Key Tasks:
    Stores data for rows to be inserted and determines statement type.


General Approach:
- File Persistence: 
    The database pages are read from and written to disk via the Pager. Rows are serialized into byte arrays to efficiently manage storage.
- Cursor Abstraction: 
    To access and manipulate rows, we use the Cursor, which isolates the logic of navigating through pages and rows from higher-level code.
- Virtual Machine Execution: 
    SQL statements are parsed and then executed by the VirtualMachine, which performs table operations using the Cursor and Row for data access and manipulation.
- REPL Interface: 
    The REPL class handles the user interaction, reading commands and passing them for execution.

Note that for now, i have a hard coded and single table in my database. For now, it:
supports two operations: inserting a row and printing all rows
supports a single, hard-coded table.

Here is what i want to do next : i want to change the format of my table from an unsorted array of rows to a B-Tree. How can i do that ? 