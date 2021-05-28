package cn.edu.thssdb.exception;

public class TableExistException extends RuntimeException {
    String tableName;
    String databaseName;

    public TableExistException(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getMessage() { return "Exception: table " + tableName + " exists in " + databaseName; }
}
