package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.type.ConditionOp;

public class Where {
    private Where left;
    private Where right;
    private ConditionOp op;
    private Condition condition;

    public Where(Where left, Where right, ConditionOp op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public Where(Condition condition) {
        this.condition = condition;
    }

}
