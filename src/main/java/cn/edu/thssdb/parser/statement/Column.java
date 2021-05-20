package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.type.ColumnConstraint;
import cn.edu.thssdb.type.ColumnTypeWithLength;

import java.util.List;

public class Column {
    private String tableName;
    private String columnName;
    private ColumnTypeWithLength columnType;
    private boolean primary;
    private boolean notNull;

    public Column(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public Column(String columnName, ColumnTypeWithLength columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public Column(String columnName, ColumnTypeWithLength columnType, List<ColumnConstraint> constraints) {
        this.columnName = columnName;
        this.columnType = columnType;

        for (ColumnConstraint constraint : constraints) {
            if (constraint == ColumnConstraint.PRIMARY) {
                primary = true;
                notNull = true;
            } else if (constraint == ColumnConstraint.NOTNULL) {
                notNull = true;
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnTypeWithLength getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnTypeWithLength columnType) {
        this.columnType = columnType;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }
}
