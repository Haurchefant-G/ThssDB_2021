package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.Where;

public class DeleteStatement extends Statement {
    String tableName;
    Where condition;

    public DeleteStatement(String tableName, Where condition) {
        this.tableName = tableName;
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Where getCondition() {
        return condition;
    }

    public void setCondition(Where condition) {
        this.condition = condition;
    }

    public StatementType getType() { return StatementType.DELETE; }
}
