package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public int columnFind(String name) {
    // TODO
    Iterator<Column> it = columns.iterator();
    for (int i = 0; it.hasNext(); ++i) {
      String cName = it.next().getName();
      if (cName.equals(name) || (tableName+"."+cName).equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public void addToStringList(List<String> l) {
    Iterator<Column> it = columns.iterator();
    for (int i = 0; it.hasNext(); ++i) {
      String cName = it.next().getName();
      l.add(tableName + '.' + cName);
    }
  }

  public String columnName(int index, boolean prefix) {
    if (prefix) {
      return tableName + '.' + columns.get(index).getName();
    } else {
      return columns.get(index).getName();
    }
  }

  public String getTableName() {
    return tableName;
  }

  public List<Column> getColumns() {
    return columns;
  }
}
