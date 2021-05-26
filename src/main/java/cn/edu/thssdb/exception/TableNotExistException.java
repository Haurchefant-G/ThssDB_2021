package cn.edu.thssdb.exception;

public class TableNotExistException extends RuntimeException {
    String databaseName;
    String tableName;

    public TableNotExistException(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public String getMessage() { return "Exception: table " + tableName + " not exists in " + databaseName; }
}
