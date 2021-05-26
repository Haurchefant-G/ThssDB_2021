package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.Value;

import java.util.List;

public class InsertStatement extends Statement {
    String tableName;
    List<String> columnNames;
    List<List<Value>> values;

    public InsertStatement(String tableName, List<String> columnNames, List<List<Value>> values) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.values = values;
    }

    public StatementType getType() { return StatementType.INSERT; }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<List<Value>> getValues() {
        return values;
    }

    public void setValues(List<List<Value>> values) {
        this.values = values;
    }
}
