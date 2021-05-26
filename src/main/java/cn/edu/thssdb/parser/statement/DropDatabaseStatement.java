package cn.edu.thssdb.parser.statement;

public class DropDatabaseStatement extends Statement {
    String databaseName;

    public DropDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public StatementType getType() { return StatementType.DROP_DATABASE; }
}
