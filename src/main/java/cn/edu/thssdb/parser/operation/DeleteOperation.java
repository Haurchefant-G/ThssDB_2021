package cn.edu.thssdb.parser.statement;

public class DeleteStatement {
    private String tableName;
    private Condition condition;

    public DeleteStatement(String tableName, Condition condition) {
        this.tableName = tableName;
        this.condition = condition;
    }
}
