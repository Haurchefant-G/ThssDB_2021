package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ConditionOp;

import java.util.List;
import java.util.function.Predicate;

public class Where {
    private Where left = null;
    private Where right = null;
    private ConditionOp op;
    private Condition condition;

    public Where(Where left, Where right, ConditionOp op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public Where(Condition condition) { this.condition = condition; }

    public boolean isTerminal() { return right==null; }

    public Predicate<Row> parse(List<MetaInfo> metaInfoList) {
        if (isTerminal()) {
            return condition.parse(metaInfoList);
        } else {
            switch (op) {
                case AND:
                    return left.parse(metaInfoList).and(right.parse(metaInfoList));
                case OR:
                    return left.parse(metaInfoList).or(right.parse(metaInfoList));
                default:
                    return null;
            }
        }
    }

}
