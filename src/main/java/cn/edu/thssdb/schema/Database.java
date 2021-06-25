package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.TableMetaVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;
  private boolean open;
  private int sessionNum;

  /**
   * 创建数据库
   * @param name 数据库名
   */
  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.open = false;
    this.sessionNum = 0;
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
        Iterator<Column> i = table.getColumns().iterator();
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
      fw.close();
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
    if (tables.containsKey(name)) {
      throw new TableExistException(name, this.name);
    }
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
      tables.clear();
    }
    File f = new File(name + ".db");
    f.delete();
    lock.writeLock().unlock();
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
        MetaInfo meta = tableMeta.visitCreate_table_stmt(parser.create_table_stmt());
        Table table = new Table(name, meta.getTableName(), meta.getColumns());
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
    if (sessionNum == 0) {
      lock.writeLock().lock();
      if (open) {
        persist();
        for (Table table : tables.values()) {
          table.close();
        }
        tables.clear();
        open = false;
      }
      lock.writeLock().unlock();
    }
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
    return table.getColumns();
  }

  public void commitTable(String tablename) {
    tables.get(tablename).commit();
  }


  public HashMap<String, Table> getTables() {
    return tables;
  }

  public Table getTable(String tableName) {
    try {
      lock.readLock().lock();
      Table table = tables.get(tableName);
      if (table == null) {
        throw new TableNotExistException(name, tableName);
      }
      return table;
    } finally {
      lock.readLock().unlock();
    }
  }

  public QueryResult select(List<QueryTable> queryTables, List<String> columnNames, Where where, boolean distinct) {
    try {
      lock.readLock().lock();
      return new QueryResult(queryTables, columnNames, where, distinct);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void registerSession() {
    ++sessionNum;
  }

  public void logoutSession() {
    --sessionNum;
  }

  public int getSessionNum() {
    return sessionNum;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    quit();
  }
}
