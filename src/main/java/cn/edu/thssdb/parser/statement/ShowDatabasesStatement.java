package cn.edu.thssdb.parser.statement;

public class ShowDatabasesStatement extends Statement {

    public StatementType getType() { return StatementType.SHOW_DATABASE; }
}
