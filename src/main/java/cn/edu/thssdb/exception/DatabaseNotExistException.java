package cn.edu.thssdb.exception;

public class DatabaseNotExistException extends RuntimeException {
    String databaseName;

    public DatabaseNotExistException(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String getMessage() { return "Exception: database " + databaseName + " not found"; }
}
