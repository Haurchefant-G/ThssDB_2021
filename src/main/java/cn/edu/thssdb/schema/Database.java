package cn.edu.thssdb.schema;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.TableMetaVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;
  private boolean open;

  /**
   * 创建数据库
   * @param name 数据库名
   */
  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.open = false;
  }

  /**
   * 获取数据库名
   * @return 数据库名
   */
  public String getName() {
    return this.name;
  }

  /**
   * 持久化数据库.db元数据，包含该数据库中所有表及表头结构，一行为一个sql创建表的命令
   */
  private void persist() {
    // TODO
    try {
      FileWriter fw = new FileWriter(name + ".db");
      BufferedWriter bw = new BufferedWriter(fw);
      for(Table table : tables.values()) {
        String line = "CREATE TABLE " + table.getTableName() + "(";
        ArrayList<String> primary = new ArrayList<String>();
        Iterator<Column> i = table.columns.iterator();
        while(i.hasNext()) {
          Column column = i.next();
          String c = column.getName() + " " + column.getType().toString();
          if (column.getType() == ColumnType.STRING)
            c += "(" + column.getMaxLength() + ")";
          if (column.getPrimary() > 0)
            primary.add(column.getName());
          else if (column.NotNull())
            c += " NOT NULL";
          c += ",";
          line += c;
        }
        line += " PRIMARY KEY(" + String.join(",", primary) + "));";
        bw.write(line);
        bw.newLine();
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 新建一张表
   * @param name 表明
   * @param columns 表头
   */
  public void create(String name, ArrayList<Column> columns) {
    // TODO
    lock.writeLock().lock();
    Table table = new Table(this.name, name, columns);
    tables.put(name, table);
    persist();
    lock.writeLock().unlock();
  }

  /**
   * 删除一张表
   * @param name 表名
   */
  public void drop(String name) {
    // TODO
    lock.writeLock().lock();
    Table table = tables.remove(name);
    table.clear();
    persist();
    lock.writeLock().unlock();

  }

  /**
   * 清空删除数据库
   */
  public void clear() {
    lock.writeLock().lock();
    for(String name: tables.keySet()) {
      drop(name);
      tables = null;
    }
    File f = new File(name + ".db");
    f.delete();
    lock.writeLock().unlock();
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  /**
   * 启动数据库
   */
  public void start() {
    lock.writeLock().lock();
    if (!open) {
      recover();
      open = true;
    }
    lock.writeLock().unlock();
  }

  /**
   * 从数据库.db元数据恢复数据库中的表
   */
  private void recover() {
    // TODO
    File f = new File(name + ".db");
    if(!f.exists()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      FileReader fr = new FileReader(f);
      BufferedReader br = new BufferedReader(fr);
      String line = br.readLine();
      TableMetaVisitor tableMeta = new TableMetaVisitor();
      while(line != null && !line.equals("")) {
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        Table table =tableMeta.visitCreate_table_stmt(parser.create_table_stmt());
        table.setDatabaseName(name);
        tables.put(table.getTableName(), table);
        line = br.readLine();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 退出关闭该数据库，并关闭其中表，保存元数据
   */
  public void quit() {
    // TODO
    lock.writeLock().lock();
    if (open) {
      persist();
      for(Table table: tables.values()) {
        table.close();
      }
      tables = null;
      open = false;
    }
    lock.writeLock().unlock();
  }

  /**
   * 获取单张表表头
   * @param tableName
   * @return
   */
  public ArrayList<Column> getColumns(String tableName) {
    Table table = tables.get(tableName);
    if (table == null) {
      return null;
    }
    return table.columns;
  }

  /**
   * 向一张表插入一行
   * @param tableName
   * @param row
   */
  public void insertRow(String tableName, Row row) {
    try {
      tables.get(tableName).insert(row);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 删除多行
   * @param tableName
   * @param rows
   */
  public void deleteRows(String tableName, ArrayList<Row> rows) {
    try {
      Table table = tables.get(tableName);
      for(Row r:rows) {
        table.delete(r);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void updateRows(String tableName, ArrayList<Row> oldrows, ArrayList<Row> newrows) {
    try {
      Table table = tables.get(tableName);
      Iterator<Row> i;
      Iterator<Row> j;
      for(i = oldrows.iterator(), j = newrows.iterator(); i.hasNext();) {
        Row oldrow = i.next();
        Row newrow = j.next();
        table.update(oldrow, newrow);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public QueryTable getTabelQuery(String tablename) {
    Table table = tables.get(tablename);
    return new QueryTable(table);
  }

  public void commitTable(String tablename) {
    tables.get(tablename).commit();
  }
}
