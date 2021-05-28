package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.Where;

public class DeleteStatement extends Statement {
    String tableName;
    Where where;

    public DeleteStatement(String tableName, Where condition) {
        this.tableName = tableName;
        this.where = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public StatementType getType() { return StatementType.DELETE; }
}
