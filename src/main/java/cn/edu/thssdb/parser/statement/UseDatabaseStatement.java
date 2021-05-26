package cn.edu.thssdb.parser.statement;

public class UseDatabaseStatement extends Statement {
    String databaseName;

    public UseDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public StatementType getType() { return StatementType.USE_DATABASE; }
}
