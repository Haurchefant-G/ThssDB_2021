package cn.edu.thssdb.parser.statement;

public class CommitStatement extends Statement{
    public StatementType getType() { return StatementType.COMMIT; }
}
