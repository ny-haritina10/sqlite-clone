Part 1 & 2 : 
    - Input handling: Encapsulated in the InputBuffer class.
    - Meta-command handling: Done by the MetaCommandHandler.
    - SQL parsing: Moved to the SQLCompiler.
    - Execution: Performed by the VirtualMachine.
    - Main application logic: Managed by REPL class.

Part 3 : 
    - In Memory (RAM), Append only, Single Table database
    - Support two operations: insert a row and printing all rows
    - reside only in memory (no disk persistence)
    - support a single hard coded table

Part 4 :
    - Data persistence 
    - Adding an abstraction to manage data writing and reading on a file (Pager)
        -> handle interaction between memory in cache and the disk
        -> Fetch page from Memory or Disk
        -> Write Page on disk

Part 5 : 
    - Adding a Cursor object to access Row in the Table
    - Refactor code to make the code more flexible for next implementation (B-Tree)