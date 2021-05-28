package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ValueType;

import java.util.List;
import java.util.function.Function;

public class Expression {
    public enum ExpressionOp {
        ADD, SUB, MUL, DIV
    }

    private Expression left = null;
    private Expression right = null;
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

    public Value getValue() {
        return value;
    }

    public boolean isTerminal() { return right == null; }

    public Function<Row, Comparable> parse(List<MetaInfo> metaInfoList) {
        if (isTerminal()) {
            if (value.getType() == ValueType.COLUMN) {
                String colName = value.getValue().toString();
                if (!colName.contains(".")) {
                    int offset = 0;
                    for (MetaInfo metaInfo : metaInfoList) {
                        int columnIdx = metaInfo.columnFind(colName);
                        if (columnIdx != -1) {
                            int finalOffset = offset;
                            return r -> r.valueOf(columnIdx + finalOffset);
                        }
                        offset += metaInfo.getColumns().size();
                    }
                    throw new ColumnNotExistException(colName);
                } else {
                    String[] names = colName.split("\\.");
                    if (names.length != 2) {
                        throw new ColumnNotExistException(colName);
                    }
                    String tableName = names[0];
                    String tableColName = names[1];
                    int offset = 0;
                    for (MetaInfo metaInfo : metaInfoList) {
                        if (metaInfo.getTableName().equals(tableName)) {
                            int columnIdx = metaInfo.columnFind(tableColName);
                            if (columnIdx != -1) {
                                int finalOffset = offset;
                                return r -> r.valueOf(columnIdx + finalOffset);
                            }
                        }
                        offset += metaInfo.getColumns().size();
                    }
                }
            } else {
                return r -> value.getValue();
            }
        } else {
            Function<Row, Comparable> expr1 = left.parse(metaInfoList);
            Function<Row, Comparable> expr2 = right.parse(metaInfoList);

            switch (op) {
                case ADD:
                    return r -> {
                        Comparable comp1 = expr1.apply(r);
                        Comparable comp2 = expr2.apply(r);
                        double value1 = ((Number)comp1).doubleValue();
                        double value2 = ((Number)comp2).doubleValue();
                        return value1 + value2;
                    };
                case SUB:
                    return r -> {
                        Comparable comp1 = expr1.apply(r);
                        Comparable comp2 = expr2.apply(r);
                        double value1 = ((Number)comp1).doubleValue();
                        double value2 = ((Number)comp2).doubleValue();
                        return value1 - value2;
                    };
                case MUL:
                    return r -> {
                        Comparable comp1 = expr1.apply(r);
                        Comparable comp2 = expr2.apply(r);
                        double value1 = ((Number)comp1).doubleValue();
                        double value2 = ((Number)comp2).doubleValue();
                        return value1 * value2;
                    };
                case DIV:
                    return r -> {
                        Comparable comp1 = expr1.apply(r);
                        Comparable comp2 = expr2.apply(r);
                        double value1 = ((Number)comp1).doubleValue();
                        double value2 = ((Number)comp2).doubleValue();
                        return value1 / value2;
                    };
                default:
                    return null;
            }
        }
        return null;
    }
}
