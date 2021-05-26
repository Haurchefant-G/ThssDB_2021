package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.Comparator;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Condition {
    private Expression left;
    private Expression right;
    private Comparator comparator;

    public Condition(Expression left, Expression right, Comparator comparator) {
        this.left = left;
        this.right = right;
        this.comparator = comparator;
    }

    public Predicate<Row> parse(List<MetaInfo> metaInfoList) {
        Function<Row, Comparable> expr1 = left.parse(metaInfoList);
        Function<Row, Comparable> expr2 = right.parse(metaInfoList);

        switch (comparator) {
            case EQ:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return v1.equals(v2);
                    } else {
                        return ((Number) v1).doubleValue() == ((Number) v2).doubleValue();
                    }
                };
            case NE:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return !v1.equals(v2);
                    } else {
                        return ((Number) v1).doubleValue() != ((Number) v2).doubleValue();
                    }
                };
            case GT:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) > 0;
                    } else {
                        return ((Number) v1).doubleValue() > ((Number) v2).doubleValue();
                    }
                };
            case GE:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) >= 0;
                    } else {
                        return ((Number) v1).doubleValue() >= ((Number) v2).doubleValue();
                    }
                };
            case LE:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) <= 0;
                    } else {
                        return ((Number) v1).doubleValue() <= ((Number) v2).doubleValue();
                    }
                };
            case LT:
                return r -> {
                    Comparable v1 = expr1.apply(r);
                    Comparable v2 = expr2.apply(r);
                    if (v1 instanceof String) {
                        return v1.compareTo(v2) < 0;
                    } else {
                        return ((Number) v1).doubleValue() < ((Number) v2).doubleValue();
                    }
                };
            default:
                return null;
        }
    }
}
