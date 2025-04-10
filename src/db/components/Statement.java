package db.components;

import db.data.Row;
import db.enums.StatementType;

public class Statement {

    private StatementType type;
    private Row rowToInsert;

    public StatementType getType() 
    { return type; }

    public void setType(StatementType type) 
    { this.type = type; }

    public Row getRowToInsert() 
    { return rowToInsert; }

    public void setRowToInsert(Row rowToInsert) 
    { this.rowToInsert = rowToInsert; }
}