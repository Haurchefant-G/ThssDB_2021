package cn.edu.thssdb.parser.statement;

public class CreateDatabaseStatement {
    private String databaseName;

    public CreateDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
