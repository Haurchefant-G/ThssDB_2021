package cn.edu.thssdb.exception;

public class DatabaseBeingUsedException extends RuntimeException {
    String databaseName;

    public DatabaseBeingUsedException(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String getMessage() { return "Exception: database " + databaseName + " is being used"; }
}
