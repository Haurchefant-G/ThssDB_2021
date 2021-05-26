package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;

public class CreateTableStatement extends Statement {
    String tableName;
    ArrayList<Column> columnList;

    public CreateTableStatement(String tableName, ArrayList<Column> columnList) {
        this.tableName = tableName;
        this.columnList = columnList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ArrayList<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(ArrayList<Column> columnList) {
        this.columnList = columnList;
    }

    public StatementType getType() { return StatementType.CREATE_TABLE; }

}
