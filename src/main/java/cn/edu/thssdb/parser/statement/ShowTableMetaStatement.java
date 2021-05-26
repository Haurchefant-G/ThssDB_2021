package cn.edu.thssdb.parser.statement;

public class ShowTableMetaStatement extends Statement {
    String tableName;

    public ShowTableMetaStatement(String tableName) {
        this.tableName = tableName;
    }

    public StatementType getType() { return StatementType.SHOW_TABLE_META; }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
