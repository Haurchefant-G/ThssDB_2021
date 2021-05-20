package cn.edu.thssdb.parser.statement;

public class DropTableStatement {
    private String tableName;

    public DropTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
