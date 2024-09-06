package db.components;

import db.enums.StatementType;

public class Statement {

    private StatementType type;

    public StatementType getType() 
    { return type; }

    public void setType(StatementType type) 
    { this.type = type; }
}