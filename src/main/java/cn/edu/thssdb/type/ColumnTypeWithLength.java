package cn.edu.thssdb.type;

public class ColumnTypeWithLength {
    private ColumnType columnType;
    private int length = 0;

    public ColumnTypeWithLength(ColumnType columnType) {
        this.columnType = columnType;
    }

    public ColumnTypeWithLength(ColumnType columnType, int length) {
        this.columnType = columnType;
        this.length = length;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
