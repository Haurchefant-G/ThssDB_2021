package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Test;


public class ValueTest {

    @Test
    public void test1() {
        Column column = new Column("col", ColumnType.INT, 1, true, 4);
        Comparable value = 123;
        value = Value.adaptToColumnType(value, column);
        assert value.equals(123);
    }

    @Test
    public void test2() {
        Column column = new Column("col", ColumnType.DOUBLE, 1, true, 8);
        Comparable value = 123;
        value = Value.adaptToColumnType(value, column);
        assert value.equals(123.0);
    }

    @Test
    public void test3() {
        Column column = new Column("col", ColumnType.STRING, 1, true, 4);
        Comparable value = "123";
        value = Value.adaptToColumnType(value, column);
        assert value.equals("123");
    }

    @Test
    public void test4() {
        Column column = new Column("col", ColumnType.LONG, 1, true, 8);
        Comparable value = 123;
        value = Value.adaptToColumnType(value, column);
        assert value.equals(123l);
    }

    @Test
    public void test5() {
        Column column = new Column("col", ColumnType.FLOAT, 1, true, 4);
        Comparable value = 123;
        value = Value.adaptToColumnType(value, column);
        assert value.equals(123f);
    }
}
