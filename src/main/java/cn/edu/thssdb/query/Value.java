package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.StringLengthTooLongException;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ValueType;

public class Value {
    private ValueType type;
    private Comparable value;

    public Value(String value, ValueType valueType) {
        this.type = valueType;
        switch (valueType) {
            case STRING:
            case COLUMN:
                this.value = value;
                break;
            case INT:
                this.value = Integer.parseInt(value);
                break;
            case DOUBLE:
                this.value = Double.parseDouble(value);
                break;
            case NULL:
                this.value = null;
                break;
        }
    }

    public static Comparable adaptToColumnType(Comparable value, Column column) {
        switch (column.getType()) {
            case INT:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case STRING:
                if (((String) value).length() > column.getMaxLength()) {
                    throw new StringLengthTooLongException(((String) value).length(), column.getMaxLength());
                }
                return (String) value;
            default:
                return value;
        }
    }

    public ValueType getType() {
        return type;
    }

    public void setType(ValueType type) {
        this.type = type;
    }

    public Comparable getValue() {
        return value;
    }

    public void setValue(Comparable value) {
        this.value = value;
    }
}
