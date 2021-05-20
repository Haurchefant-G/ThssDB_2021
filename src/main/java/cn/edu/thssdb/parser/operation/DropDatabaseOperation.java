package cn.edu.thssdb.parser.statement;

public class DropDatabaseStatement {
    private String databaseName;

    public DropDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
