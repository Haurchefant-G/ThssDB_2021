package cn.edu.thssdb.query;

import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.ToIntBiFunction;

public class QueryTable implements Iterator<Row> {

  private List<Column> columns;
  private BPlusTreeIterator<Entry, Row> iterator;
  private String tableName;
  public QueryTable() {
    // 得传一张表才能构造 设为private 不让人调用
  }

  public QueryTable(Table table) {
    this.columns = table.columns;
    this.tableName = table.getTableName();
    this.iterator = table.index.iterator();
  }

  //public QueryTable(ArrayList<String> tableName)

  @Override
  public boolean hasNext() {
    // TODO
    return iterator.hasNext();
  }

  @Override
  public Row next() {
    // TODO
    return iterator.next().getValue();
  }

  public String getTableName() {
    return tableName;
  }

  public int getColumnIndex(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public ColumnType getColunmnType(int index) {
    return columns.get(index).getType();
  }

  public void setWhere(int index, String comparator, String value) {
    BiPredicate<Comparable, Comparable> com;
    switch (comparator) {
      case "=":
        com = Comparable::equals;
        break;
      case  "<>":
        com = (x,y) -> !x.equals(y);
        break;
      case "<":
        com = (x,y) -> x.compareTo(y) < 0;
        break;
      case ">":
        com = (x,y) -> x.compareTo(y) > 0;
        break;
      case "<=":
        com = (x,y) -> x.compareTo(y) <= 0;
        break;
      case ">=":
        com = (x,y) -> x.compareTo(y) >= 0;
        break;
      default:
        com = (x,y) -> false;
    }
    Comparable v;
    switch (columns.get(index).getType()) {
      case INT:
        v = Integer.parseInt(value);
        break;
      case FLOAT:
        v = Float.parseFloat(value);
        break;
      case DOUBLE:
        v = Double.parseDouble(value);
        break;
      case LONG:
        v = Long.parseLong(value);
        break;
      default:
        v = value;
        break;
    }
  }

  public ArrayList<Row> queryRow(int index, String comparator, String value) {
    ArrayList<Row> rows = new ArrayList<>();
    BiPredicate<Comparable, Comparable> com;
    switch (comparator) {
      case "=":
        com = Comparable::equals;
        break;
      case  "<>":
        com = (x,y) -> !x.equals(y);
        break;
      case "<":
        com = (x,y) -> x.compareTo(y) < 0;
        break;
      case ">":
        com = (x,y) -> x.compareTo(y) > 0;
        break;
      case "<=":
        com = (x,y) -> x.compareTo(y) <= 0;
        break;
      case ">=":
        com = (x,y) -> x.compareTo(y) >= 0;
        break;
      default:
        com = (x,y) -> false;
    }
    Comparable v;
    switch (columns.get(index).getType()) {
      case INT:
        v = Integer.parseInt(value);
        break;
      case FLOAT:
        v = Float.parseFloat(value);
        break;
      case DOUBLE:
        v = Double.parseDouble(value);
        break;
      case LONG:
        v = Long.parseLong(value);
        break;
      default:
        v = value;
        break;
    }
    for (Row r = this.next(); this.hasNext();) {
      if (com.test(r.getEntry(index).value, v)) {
        rows.add(r);
      }
    }
    if (rows.size() == 0) {
      return null;
    }
    return rows;
  }
}
