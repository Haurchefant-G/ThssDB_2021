package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException {
    String colName;

    public DuplicateColumnException(String colName) {
        this.colName = colName;
    }

    @Override
    public String getMessage() { return "Exception: duplicate column name " + this.colName; }
}
