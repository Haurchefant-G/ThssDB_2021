package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.type.Comparator;

public class Condition {
    private Expression left;
    private Expression right;
    private Comparator comparator;

    public Condition(Expression left, Expression right, Comparator comparator) {
        this.left = left;
        this.right = right;
        this.comparator = comparator;
    }
}
