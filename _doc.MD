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