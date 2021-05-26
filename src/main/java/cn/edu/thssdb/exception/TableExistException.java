package cn.edu.thssdb.exception;

public class TableExistException extends RuntimeException {
    String tableName;

    public TableExistException(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
