package cn.edu.thssdb.parser.statement;

import java.util.List;

public class CreateTableStatement {
    private String tableName;
    private List<Column> columns;
    private String primaryKey;

    public CreateTableStatement(String tableName, List<Column> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKey = primaryKey;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
