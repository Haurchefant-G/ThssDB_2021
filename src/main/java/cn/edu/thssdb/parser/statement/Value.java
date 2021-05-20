package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.type.ValueType;

public class Value {
    private ValueType type;
    private Comparable value;
    private Column column;

    public Value(String value, ValueType valueType) {
        this.type = valueType;
        switch (valueType) {
            case STRING:
            case COLUMN:
                this.value = value;
                break;
            case INT:
                this.value = Integer.parseInt(value);
            case DOUBLE:
                this.value = Double.parseDouble(value);
            case NULL:
                this.value = null;
        }
    }

    public Value(Column column) {
        this.column = column;
        this.type = ValueType.COLUMN;
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
