package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.query.Expression;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.Value;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.LockType;
import cn.edu.thssdb.utils.FileStorage;
import cn.edu.thssdb.utils.Page;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private String databaseName;
  private String tableName;
  private ArrayList<Column> columns;
  private BPlusTree<Entry, Row> index;
  private int primaryIndex;
  FileStorage fileStorage;

  LockType lockType;
  public ArrayList<Long> S_lock;
  public ArrayList<Long> X_lock;

  /**
   * 创建表并设置表所在的数据库
   * @param databaseName 数据库名
   * @param tableName 表名
   * @param columns 表头
   */
  public Table(String databaseName, String tableName, ArrayList<Column> columns) {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = columns;
    this.primaryIndex = 0;
    for (int i = 1; i < columns.size(); ++i) {
      if (columns.get(this.primaryIndex).getPrimary() < columns.get(i).getPrimary()) {
        this.primaryIndex = i;
      }
    }
    this.S_lock = new ArrayList<Long>();
    this.X_lock = new ArrayList<Long>();
    this.lockType = LockType.NONE;

    try {
      recover();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 设置表所在的数据库
   * @param name 数据库名
   */
  public void setDatabaseName(String name) {
    this.databaseName = name;
  }

  /**
   * 获取表的名字
   * @return 表明
   */
  public String getTableName() {
    return this.tableName;
  }

  public int getIndexSize() { return index.size(); }

  public ArrayList<Column> getColumns() { return columns; }

  public void insert(List<String> columnNames, List<List<Value>> values) throws IOException {
    try {
      lock.writeLock().lock();
      int colNum = columnNames.size();
      // TODO: all filled check
      boolean insertAll = (colNum == 0);

      for (List<Value> colValues : values) {
        if (insertAll && columns.size() != colValues.size()) {
          throw new InsertColumnSizeNotMatchException(columns.size(), colValues.size());
        } else if (!insertAll && colNum != colValues.size()) {
          throw new InsertColumnSizeNotMatchException(colNum, colValues.size());
        }

        ArrayList<Entry> entryList = new ArrayList<>();

        for(int i = 0; i < columns.size(); i++) {
          Column tableColumn = columns.get(i);
          boolean filled = false;
          if (insertAll) {
            Value value = colValues.get(i);
            if (!ColumnType.columnValueTypeCheck(tableColumn.getType(), value.getType())) {
              throw new TypeConflictException(tableColumn.getName(), tableColumn.getType(), value.getValue());
            }
            entryList.add(new Entry(value.getValue()));
            continue;
          }

          for (int j = 0; j < colNum; j++) {
            if (columnNames.get(j).equals(tableColumn.getName())) {
              Value value = colValues.get(j);
              if (!ColumnType.columnValueTypeCheck(tableColumn.getType(), value.getType())) {
                throw new TypeConflictException(tableColumn.getName(), tableColumn.getType(), value.getValue());
              }
              entryList.add(new Entry(value.getValue()));
              filled = true;
              break;
            }
          }

          if (!filled) {
            if (tableColumn.isNotNull()) {
              throw new NotNullException(tableColumn.getName());
            } else {
              entryList.add(new Entry(null));
            }
          }
        }
        Row row = new Row(entryList);
        insert(row);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int update(String columnName, Expression expression, Where condition) throws IOException {
    try {
      lock.writeLock().lock();
      Column column = null;
      int colIdx;
      for (colIdx = 0; colIdx < columns.size(); colIdx++) {
        if (columnName.equals(columns.get(colIdx).getName())) {
          column = columns.get(colIdx);
          break;
        }
      }
      if (column == null) {
        throw new ColumnNotExistException(columnName);
      }

      List<MetaInfo> metaInfos = new ArrayList<MetaInfo>() {{
        add(getMetaInfo());
      }};

      Predicate<Row> predicate = null;
      if (condition != null) {
        predicate = condition.parse(metaInfos);
      }
      Function<Row, Comparable> func = expression.parse(metaInfos);
      int updateNum = 0;
      for (Row row : this) {
        if (predicate == null || predicate.test(row)) {
          updateNum++;
          Comparable value = func.apply(row);
          value = Value.adaptToColumnType(value, column);
          Entry newEntry = new Entry(value);
          if (column.isPrimary() && index.contains(newEntry)) {
            throw new PrimaryKeyViolationException();
          }

          Entry oldEntry = row.getEntry(primaryIndex);
          row.entries.set(colIdx, newEntry);
          fileStorage.updateRowInPage(row.page, row.offset, row);
          if (column.isPrimary()) {
            index.remove(oldEntry);
            index.put(newEntry, row);
          }
        }
      }
      return updateNum;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 读取index文件恢复BPlusTree的函数接口
   */
  @FunctionalInterface
  interface readIndex {
    Comparable read() throws IOException;
  };

  /**
   * 将BPlusTree结点写入index文件的函数接口
   */
  @FunctionalInterface
  interface writeIndex {
    void write(Comparable dataouput) throws IOException;
  };

  public int add_S_lock(long session){
    if (lockType.equals(LockType.X))
    {
      if (X_lock.contains(session))
        return 0;
      else
        return -1;
    }
    else if(lockType.equals(LockType.S))
    {
      if (S_lock.contains(session))
        return 0;
      else
      {
        S_lock.add(session);
        lockType = LockType.S;
        return 1;
      }
    }
    else
    {
      S_lock.add(session);
      lockType = LockType.S;
      return 1;
    }
  }

  public int add_X_lock(long session){
    if(lockType.equals(LockType.X))
    {
      if(X_lock.contains(session))
        return 0;
      else
        return -1;
    }
    else if(lockType.equals(LockType.S))
      return -1;
    else {
      X_lock.add(session);
      lockType = LockType.X;
      return 1;
    }
  }

  public void free_S_lock(long session){
    if(S_lock.contains(session))
    {
      S_lock.remove(session);

      if(S_lock.size() == 0)
        lockType = LockType.NONE;
      else
        lockType = LockType.S;
    }
  }

  public void free_X_lock(long session){
    if(X_lock.contains(session))
    {
      X_lock.remove(session);

      lockType = LockType.NONE;
    }
  }

  /**
   * 恢复table，创建对应的存储管理，根据index文件恢复BPlustree
   * @throws IOException
   */
  private void recover() throws IOException {
    // TODO
    this.index = new BPlusTree<>();
    fileStorage = new FileStorage(tableName + ".table", columns);
    File f = new File(tableName + ".index");
    try {
      f.createNewFile();
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    FileInputStream indexFile = new FileInputStream(tableName + ".index");
    DataInputStream indexInput = new DataInputStream(indexFile);
    ColumnType indexColumn = columns.get(primaryIndex).getType();
    readIndex readindex;
    switch (indexColumn) {
      case STRING:
        readindex = indexInput::readUTF;
        break;
      case DOUBLE:
        readindex = indexInput::readDouble;
        break;
      case FLOAT:
        readindex = indexInput::readFloat;
        break;
      case LONG:
        readindex = indexInput::readLong;
        break;
      case INT:
      default:
        readindex = indexInput::readInt;
        break;
    }
    Comparable value;
    int page, offset;
    while(true) {
      try {
        value = readindex.read();
        page = indexInput.readInt();
        offset = indexInput.readInt();
      } catch (EOFException e) {
        break;
      }
      index.put(new Entry(value), fileStorage.searchRowInPage(page, offset));
    }
    indexInput.close();
    indexFile.close();
  }

  /**
   * 清空删除表
   */
  void clear() {
    fileStorage.close();
    fileStorage = null;
    columns = null;
    index = null;
    File f = new File(tableName + ".table");
    f.delete();
    File f2 = new File(tableName + ".index");
    f2.delete();
  }

  private MetaInfo getMetaInfo() {
    return new MetaInfo(tableName, columns);
  }

  /**
   * 关闭表，维护存储文件和index文件
   */
  void close() {
    try {
      persist();
      fileStorage.close();
      fileStorage = null;
      columns = null;
      index = null;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void commit() {
    try {
      persist();
      fileStorage.closeFile();
      // fileStorage.openFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 持久化index文件
   * @throws IOException
   */
  private void persist() throws IOException {
    FileOutputStream indexFile = new FileOutputStream(tableName + ".index");
    DataOutputStream indexOutput = new DataOutputStream(indexFile);
    ColumnType indexColumn = columns.get(primaryIndex).getType();
    writeIndex wrtieindex;
    switch (indexColumn) {
      case STRING:
        wrtieindex = (x) -> indexOutput.writeUTF((String) x);
        break;
      case DOUBLE:
        wrtieindex = (x) -> indexOutput.writeDouble((Double) x);
        break;
      case FLOAT:
        wrtieindex = (x) -> indexOutput.writeFloat((Float) x);
        break;
      case LONG:
        wrtieindex = (x) -> indexOutput.writeLong((Long) x);
        break;
      case INT:
      default:
        wrtieindex = (x) -> indexOutput.writeInt((Integer) x);
        break;
    }
    for(BPlusTreeIterator<Entry, Row> i = index.iterator(); i.hasNext();) {
      Pair<Entry, Row> node = i.next();
      wrtieindex.write(node.getKey().value);
      indexOutput.writeInt(node.getValue().page);
      indexOutput.writeInt(node.getValue().offset);
    }
    indexOutput.close();
    indexFile.close();
  }

  public boolean hasKey(Entry key) {
    try {
      lock.readLock().lock();
      return index.contains(key);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * 插入新一行
   * @param row
   * @throws IOException
   */
  void insert(Row row) throws IOException {
    // TODO
    Entry primary = row.getEntries().get(primaryIndex);
    if (hasKey(primary)) {
      throw new DuplicateKeyException();
    }
    fileStorage.insertToPage(row);
    index.put(row.getEntry(primaryIndex), row);
  }

  /**
   * 删除一行
   * @param row
   * @throws IOException
   */
  void delete(Row row) throws IOException {
    // TODO
    fileStorage.deleteFromPage(row.page, row.offset);
    index.remove(row.getEntry(primaryIndex));
  }

  int delete(Where condition) throws IOException {
    Predicate<Row> predicate = null;
    if (condition != null) {
      predicate = condition.parse(new ArrayList<MetaInfo>() {{
        add(getMetaInfo());
      }});
    }
    try {
      lock.writeLock().lock();
      int num = 0;
      for (Row row : this) {
        if (predicate == null || predicate.test(row)) {
          delete(row);
          num++;
        }
      }
      return num;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().getValue();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
