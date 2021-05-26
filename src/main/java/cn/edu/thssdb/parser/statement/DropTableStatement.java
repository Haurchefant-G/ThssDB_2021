package cn.edu.thssdb.parser.statement;

public class DropTableStatement extends Statement {
    String tableName;

    public DropTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public StatementType getType() { return StatementType.DROP_TABLE; }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
