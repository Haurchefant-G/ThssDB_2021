package cn.edu.thssdb.parser.statement;

public class CreateUserStatement extends Statement {
    String username;
    String password;

    public CreateUserStatement(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public StatementType getType() { return StatementType.CREATE_USER; }
}
