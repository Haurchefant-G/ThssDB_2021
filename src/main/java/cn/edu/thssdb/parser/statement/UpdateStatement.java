package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.Expression;
import cn.edu.thssdb.query.Where;

public class UpdateStatement extends Statement {
    String tableName;
    String columnName;
    Expression expression;
    Where condition;

    public UpdateStatement(String tableName, String columnName, Expression expression, Where condition) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.expression = expression;
        this.condition = condition;
    }

    public StatementType getType() { return StatementType.UPDATE; }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public Expression getExpression() {
        return expression;
    }

    public Where getCondition() {
        return condition;
    }
}
