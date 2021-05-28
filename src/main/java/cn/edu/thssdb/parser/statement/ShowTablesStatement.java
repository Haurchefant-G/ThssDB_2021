package cn.edu.thssdb.parser.statement;

public class ShowTablesStatement extends Statement {
    String databaseName;

    public ShowTablesStatement() {}

    public ShowTablesStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public StatementType getType() { return StatementType.SHOW_TABLES; }

    public String getDatabaseName() {
        return databaseName;
    }
}
