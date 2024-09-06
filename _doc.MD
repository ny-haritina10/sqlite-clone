Part 1 & 2 : 
    - Input handling: Encapsulated in the InputBuffer class.
    - Meta-command handling: Done by the MetaCommandHandler.
    - SQL parsing: Moved to the SQLCompiler.
    - Execution: Performed by the VirtualMachine.
    - Main application logic: Managed by REPL class.