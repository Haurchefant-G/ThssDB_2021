package cn.edu.thssdb.parser.statement;

public class DropUserStatement extends Statement {
    String userName;

    public DropUserStatement(String userName) {
        this.userName = userName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public StatementType getType() { return StatementType.DROP_USER; }
}
