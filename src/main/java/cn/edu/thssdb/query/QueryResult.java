package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

public class QueryResult {

  private List<MetaInfo> metaInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  private Predicate<Row> predicate;

  public List<Column> columns;

  boolean distinct;

  public QueryResult(QueryTable[] queryTables) {
    // TODO
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
  }

    public QueryResult(List<QueryTable> queryTables, List<Column> columns, Where condition, boolean distinct) {
      this.distinct = distinct;
      this.columns = columns;
    }

    public static Row combineRow(LinkedList<Row> rows) {
    ArrayList<Entry> entries = new ArrayList<>();
    for(Row row: rows) {
      entries.addAll(row.getEntries());
    }

    Row combined_row = new Row();
    combined_row.appendEntries(entries);

    return combined_row;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    ArrayList<Entry> entries = new ArrayList<>();
    for(int i: index) {
      entries.add(row.getEntries().get(i));
    }

    Row queried_row = new Row();
    queried_row.appendEntries(entries);

    return queried_row;
  }
}
