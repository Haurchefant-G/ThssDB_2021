package cn.edu.thssdb.parser.statement;

public class BeginTransactionStatement extends Statement{
    public StatementType getType() { return StatementType.BEGIN_TRANSACTION; }
}
