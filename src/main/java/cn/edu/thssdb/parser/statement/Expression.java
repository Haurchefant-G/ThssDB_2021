package cn.edu.thssdb.parser.statement;

public class Expression {
    public enum ExpressionOp {
        ADD, SUB, MUL, DIV
    }

    private Expression left;
    private Expression right;
    private ExpressionOp op;
    private Value value;

    public Expression(Expression left, Expression right, ExpressionOp op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public Expression(Value value) {
        this.value = value;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    public ExpressionOp getOp() {
        return op;
    }
}
