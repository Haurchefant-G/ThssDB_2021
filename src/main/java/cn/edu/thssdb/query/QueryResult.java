package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DuplicateColumnException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.function.Predicate;

public class QueryResult {
  private List<QueryTable> queryTables;
  public List<MetaInfo> metaInfos = new ArrayList<>();
  public List<Integer> index = new ArrayList<>();
  public Predicate<Row> predicate;
  public List<Row> results = new ArrayList<>();
  Stack<Row> rows = new Stack<>();

  public List<String> columnNames;
  public boolean asterisk = false;

  boolean distinct = false;


    public QueryResult(List<QueryTable> queryTables, List<String> columnNames, Where where, boolean distinct) {
      this.queryTables = queryTables;
      this.distinct = distinct;
      this.columnNames = columnNames;

      for (QueryTable table : queryTables) {
        metaInfos.addAll(table.getMetaInfo());
      }

      if (where != null) {
        predicate = where.parse(metaInfos);
      }

      buildIndex();
      getAllQuery();
    }

    private void buildIndex() {
      if (columnNames == null) {
        asterisk = true;
        return;
      }

      for (String colName : columnNames) {
        if (!colName.contains(".")) {
          int offset = 0;
          int colMatch = 0;
          for (MetaInfo metaInfo : metaInfos) {
            int columnIdx = metaInfo.columnFind(colName);
            if (columnIdx != -1) {
              index.add(offset + columnIdx);
              colMatch += 1;
              if (colMatch > 1) {
                throw new DuplicateColumnException(colName);
              }
            }
            offset += metaInfo.getColumns().size();
          }
          if (colMatch == 0) {
            throw new ColumnNotExistException(colName);
          }
        } else {
          String[] names = colName.split("\\.");
          if (names.length != 2) {
            throw new ColumnNotExistException(colName);
          }
          String tableName = names[0];
          String tableColName = names[1];
          boolean found = false;
          int offset = 0;
          for (MetaInfo metaInfo : metaInfos) {
            if (metaInfo.getTableName().equals(tableName)) {
              int columnIdx = metaInfo.columnFind(tableColName);
              if (columnIdx != -1) {
                index.add(offset + columnIdx);
                found = true;
                break;
              }
            }
            offset += metaInfo.getColumns().size();
          }
          if (!found) {
            throw new ColumnNotExistException(colName);
          }
        }
      }
    }

    private Row getNextFullRow() {
      if (rows.isEmpty()) {
        // init
        for (QueryTable queryTable : queryTables) {
          if (!queryTable.hasNext()) {
            return null;
          }
          rows.push(queryTable.next());
        }
      } else {
        // cartesian product
        int index = rows.size() - 1;
        for (; index >= 0; index--) {
          rows.pop();
          if (!queryTables.get(index).hasNext()) {
            queryTables.get(index).refresh();
          } else {
            break;
          }
        }
        // all value has been visited
        if (index < 0) {
          return null;
        }
        for (int i = index; i < queryTables.size(); i++) {
          if (!queryTables.get(i).hasNext()) {
            break;
          }
          rows.push(queryTables.get(i).next());
        }
      }
      return combineRow(rows);
    }

    private Row getNextLegalQuery() {
      while (true) {
        Row row = getNextFullRow();
        if (row == null) {
          return null;
        }
        if (predicate != null && !predicate.test(row)) {
          continue;
        }
        return generateQueryRecord(row);
      }
    }

    public void getAllQuery() {
      while (true) {
        Row row = getNextLegalQuery();
        if (row == null) {
          break;
        }
        results.add(row);
      }
    }

    public static Row combineRow(List<Row> rows) {
    ArrayList<Entry> entries = new ArrayList<>();
    for(Row row: rows) {
      entries.addAll(row.getEntries());
    }

    Row combined_row = new Row();
    combined_row.appendEntries(entries);

    return combined_row;
  }

  public Row generateQueryRecord(Row row) {
    if (asterisk) {
      return row;
    }

    ArrayList<Entry> entries = new ArrayList<>();
    for(int i: index) {
      entries.add(row.getEntries().get(i));
    }

    Row queried_row = new Row();
    queried_row.appendEntries(entries);

    return queried_row;
  }

  public List<MetaInfo> getMetaInfos() {
    return metaInfos;
  }

  public List<Integer> getIndex() {
    return index;
  }

  public Predicate<Row> getPredicate() {
    return predicate;
  }

  public List<Row> getResults() {
    return results;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public boolean isAsterisk() {
    return asterisk;
  }

  public boolean isDistinct() {
    return distinct;
  }
}
