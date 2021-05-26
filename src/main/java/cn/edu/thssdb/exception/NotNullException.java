package cn.edu.thssdb.exception;

public class NotNullException extends RuntimeException {
    String colName;

    public NotNullException(String colName) {
        this.colName = colName;
    }

    @Override
    public String getMessage() { return "Exception: Column " + colName + " cannot be null!"; }
}
