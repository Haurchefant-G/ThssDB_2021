package cn.edu.thssdb.exception;

import cn.edu.thssdb.type.ColumnType;

public class TypeConflictException extends RuntimeException {
    String column;

    ColumnType columnType;

    Comparable value;

    public TypeConflictException(String column, ColumnType columnType, Comparable value) {
        this.column = column;
        this.columnType = columnType;
        this.value = value;
    }

    @Override
    public String getMessage() {
        return "Exception: " + value + "is invalid for Column " + column + " (TYPE: " + columnType + ").";
    }
}
