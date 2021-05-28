package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.parser.statement.Statement;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.DatabaseMetaVisitor;
import cn.edu.thssdb.server.Session;
import javafx.scene.control.Tab;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Manager {
  private final HashMap<String, Database> databases;
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final SQLProcessor sqlProcessor = new SQLProcessor(this);

  public static Database selectDatabase;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  private static ArrayList<Session> sessionList;


  public Manager() {
    // TODO
    databases = new HashMap<>();
    selectDatabase = null;

    sessionList = new ArrayList<Session>();
    recoverMeta();
    createDatabaseIfNotExists("Default");
  }

  public void addSession(long sessionId) {
    sessionList.add(new Session(sessionId));
  }

  public void deleteSession(long sessionId) {
    for (Session session: sessionList) {
      if (session.getSessionId() == sessionId) {
        session.getLock().writeLock().unlock();
        session.getLock().readLock().unlock();
        sessionList.remove(session);
        break;
      }
    }
  }

  public Session getSession(long sessionId) {
    for (Session session: sessionList) {
      if (session.getSessionId() == sessionId) {
        return session;
      }
    }
    return null;
  }

  public List<SQLResult> execute(String sql, long sessionId) {
    Session session = getSession(sessionId);
    if (session == null) {
        return Collections.singletonList(new SQLResult("Invalid session Id.", false));
    }
    List<Statement> statementList = sqlProcessor.parseSQL(sql);
    if (statementList == null || statementList.size() == 0) {
      return Collections.singletonList(new SQLResult("No sql command is parsed.", false));
    }
    return sqlProcessor.executeSQL(statementList, session);
  }

  /**
   * 创建新的数据库
   * @param name 数据库名
   */
  public void createDatabaseIfNotExists(String name) {
    try {
      lock.writeLock().lock();
      if (databases.get(name) == null) {
        Database database = new Database(name);
        databases.put(name, database);
      }
      persistMeta();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 删除数据库，若删除的是当前操作数据库,应该将当前操作数据库置为null
   * @param name
   */
  public void deleteDatabase(String name, Session session) {
    try {
      lock.writeLock().lock();
      Database databaseToDelete = databases.get(name);
      if (databaseToDelete == null) {
        throw new DatabaseNotExistException(name);
      }

      for (Session session1 : sessionList) {
        if (session1 != session && name.equals(session1.getDatabaseName())) {
          throw new DatabaseBeingUsedException(name);
        }
      }

      if (name.equals(session.getDatabaseName())) {
        session.setDatabase(null);
      }

      databases.remove(name);
      persistMeta();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 切换当前操作的数据库，恢复其元数据，并关闭之前的数据库，持久化关闭的数据库的元数据
   * @param name 数据库名
   */
  public void switchDatabase(String name, Session session) {
    if (name.equals(session.getDatabaseName())) {
      return;
    }

    try {
      session.getWriteLock().lock();
      Database newSelect = databases.get(name);
      if (newSelect == null) {
        throw new DatabaseNotExistException(name);
      }
      session.setDatabase(newSelect);
    } finally {
      session.getWriteLock().unlock();
    }
  }

  /**
   * 更新所有数据库元数据
   */
  public void persistMeta(){
    try {
      FileWriter fw = new FileWriter("manager.meta");
      BufferedWriter bw = new BufferedWriter(fw);
      for(String dbname : databases.keySet()) {
        bw.write("CREATE DATABASE " + dbname + ";");
        bw.newLine();
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 从元数据恢复所有数据库（不包含数据库内数据，因为只有操作某个数据库时才会加载那个数据库的元数据）
   */
  public void recoverMeta() {
    lock.readLock().lock();
    File f = new File("manager.meta");
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
      DatabaseMetaVisitor databaseMeta = new DatabaseMetaVisitor();
      while(line != null && !line.equals("")) {
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        Database database = databaseMeta.visitCreate_db_stmt(parser.create_db_stmt());
        databases.put(database.getName(), database);
        line = br.readLine();
      }
      fr.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Database getDatabase (String databaseName) {
    try {
      lock.readLock().lock();
      return databases.get(databaseName);
    } catch (KeyNotExistException e) {
      throw new KeyNotExistException();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public List<List<String>> getTableMeta(String tableName, Session session) {
    Database database = session.getDatabase();
    try {
      database.lock.readLock().lock();
      Table table = database.getTable(tableName);

      List<List<String>> tableMeta = new ArrayList<>();
      for (Column column : table.columns) {
        tableMeta.add(column.getMetaList());
      }
      return tableMeta;
    } finally {
      database.lock.readLock().unlock();
    }
  }

    public void createTable(String tableName, ArrayList<Column> columns, Session session) {
      Database database = session.getDatabase();
      try {
        database.lock.writeLock().lock();
        database.create(tableName, columns);
      } finally {
        database.lock.writeLock().unlock();
      }
    }

    public void createUser(String user, String password) {
    }

    public void dropUser(String username) {

    }

  public void dropTable(String tableName, Session session) {
    Database database = session.getDatabase();
    database.drop(tableName);
  }

  public QueryTable getQueryTable(TableQuery tableQuery, Session session) {
    Database database = session.getDatabase();
    try {
      database.lock.readLock().lock();
      if (tableQuery.getTableRight() == null) {
        return new SingleTable(database.getTable(tableQuery.getTableLeft()));
      } else {
        Where condition = tableQuery.getWhere();
        List<Table> tables = new ArrayList<Table>() {{
          add(database.getTable(tableQuery.getTableLeft()));
          add(database.getTable(tableQuery.getTableRight()));
        }};
        return new JointTable(tables, condition);
      }
    } finally {
      database.lock.readLock().unlock();
    }
  }

  public void insert(String tableName, List<String> columnNames, List<List<Value>> values, Session session) throws IOException {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);
    table.insert(columnNames, values);
  }

  public QueryResult select(List<String> columnNames, List<TableQuery> tableQueries, Where condition, boolean distinct, Session session) {
    Database database = session.getDatabase();
    List<QueryTable> queryTables = tableQueries.stream().map(tableQuery -> getQueryTable(tableQuery, session)).collect(Collectors.toList());
    return database.select(queryTables, columnNames, condition, distinct);
  }

  public int delete(String tableName, Where condition, Session session) {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);
    return table.delete(condition);
  }

  public int update(String tableName, String columnName, Expression expression, Where condition, Session session) {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);
    return table.update(columnName, expression, condition);
  }

  public List<String> getTables(String databaseName, Session session) {
    if (databaseName == null) {
      databaseName = session.getDatabaseName();
    }
    Database database = databases.get(databaseName);
    return Arrays.asList(database.getTables().keySet().toArray(new String[0]));
  }

  public static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
