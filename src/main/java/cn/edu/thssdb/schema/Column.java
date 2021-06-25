package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnConstraint;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ColumnTypeWithLength;

import java.util.Arrays;
import java.util.List;

public class Column implements Comparable<Column> {
    private String name;
    private ColumnType type;
    private int primary = 0;
    private boolean notNull = false;
    private int maxLength;
    private String tableName;

    public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
        this.name = name;
        this.type = type;
        this.primary = primary;
        this.notNull = notNull;
        this.maxLength = maxLength;
    }

    public Column(String columnName, String tableName) {
        this.name = columnName;
        this.tableName = tableName;
    }

    public Column(String columnName, ColumnTypeWithLength columnType, List<ColumnConstraint> constraints) {
        this.name = columnName;
        this.type = columnType.getColumnType();
        this.maxLength = columnType.getLength();

        for (ColumnConstraint constraint : constraints) {
            if (constraint == ColumnConstraint.NOTNULL) {
                notNull = true;
            }
            if (constraint == ColumnConstraint.PRIMARY) {
                notNull = true;
                primary = 1;
            }
        }
    }

    @Override
    public int compareTo(Column e) {
        return name.compareTo(e.name);
    }

    public String toString() {
        return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
    }

    public int getPrimary() {
        return primary;
    }

    public boolean isPrimary() {
        return primary != 0;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public ColumnType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public boolean NotNull() {
        return notNull;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public void setPrimary(int primary) {
        this.primary = primary;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getMetaList() {
        return Arrays.asList(
                name,
                type == ColumnType.STRING ? "STRING(" + maxLength + ")" : type.toString(),
                notNull | primary == 1 ? "YES" : "NO",
                primary == 1 ? "Primary" : ""
        );
    }
}
