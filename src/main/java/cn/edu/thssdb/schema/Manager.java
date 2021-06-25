package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.parser.statement.Statement;
import cn.edu.thssdb.parser.statement.StatementType;
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
  public TransactionManager transactionManager;
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
    
    transactionManager = new TransactionManager();
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

  public void check_WAL(List<Statement> statementList, Session session, String sql) {
    for (Statement statement : statementList)
    {
      if (statement.getType().equals(StatementType.INSERT) ||
          statement.getType().equals(StatementType.DELETE) ||
          statement.getType().equals(StatementType.UPDATE) ||
          statement.getType().equals(StatementType.SELECT) ||
          statement.getType().equals(StatementType.BEGIN_TRANSACTION) ||
          statement.getType().equals(StatementType.COMMIT))
          write_log(session, sql);
    }
  }

  /**
   * check INSERT, DELETE, UPDATE, SELECT for WAL
   */
  public List<SQLResult> execute(String sql, long sessionId) {
    Session session = getSession(sessionId);
    if (session == null) {
      return Collections.singletonList(new SQLResult("Invalid session Id.", false));
    }
    List<Statement> statementList = new ArrayList<>();

    String[] sqls = sql.split(";");
    int size = sqls.length;
    for (int i = 0; i < sqls.length; i++)
    {
      List<Statement> tmp = sqlProcessor.parseSQL(sqls[i]);
      check_WAL(tmp, session, sqls[i]);
      statementList.addAll(tmp);
    }

    if (statementList == null || statementList.size() == 0) {
      return Collections.singletonList(new SQLResult("No sql command is parsed.", false));
    }

    return sqlProcessor.executeSQL(statementList, session);
  }

  public List<SQLResult> directly_execute(String sql, long sessionId) {
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

  public void write_log(Session session, String sql)
  {
    try
    {
      Database database = session.getDatabase();
      String db_name = database.getName();
      String filename = db_name + ".log";

      FileWriter writer = new FileWriter(filename,true);
      writer.write(sql + ";\n");
      writer.close();
    } catch (IOException e) {
    }
  }

  public Integer cal_last_cmd(ArrayList<Integer> begin_transcation_list,
                              ArrayList<Integer> commit_list,
                              ArrayList<String> lines) {
    Integer last_cmd = 0;
    if (begin_transcation_list.size() == commit_list.size())
      last_cmd = lines.size();
    else
      last_cmd = begin_transcation_list.get(begin_transcation_list.size() - 1);

    return last_cmd;
  }

  public void rewrite_log(String filename, ArrayList<String> lines, Integer index) {
    try {
      FileWriter writer1 = new FileWriter(filename);
      writer1.write("");
      writer1.close();

      FileWriter writer2 = new FileWriter(filename, true);
      for (int i = 0; i < index; i++)
        writer2.write(lines.get(i) + "\n");
      writer2.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void read_log(Session session)
  {
    String db_name = session.getDatabase().getName();
    String filename = db_name + ".log";
    long session_id = session.getSessionId();

    File file = new File(filename);
    if(file.exists() && file.isFile())
    {
      directly_execute("use " + db_name, session_id);

      try {
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(reader);

        ArrayList<String> lines = new ArrayList<>();
        ArrayList<Integer> begin_transcation_list = new ArrayList<>();
        ArrayList<Integer> commit_list = new ArrayList<>();
        String line;
        int index = 0;

        while ((line = bufferedReader.readLine()) != null)
        {
          if (line.equals("begin transaction;"))
            begin_transcation_list.add(index);
          else if (line.equals("commit;"))
            commit_list.add(index);

          lines.add(line);
          index++;
        }
        reader.close();
        bufferedReader.close();


        Integer last_cmd = cal_last_cmd(begin_transcation_list, commit_list, lines);
        for (int i = 0; i < last_cmd; i++)
          directly_execute(lines.get(i), session_id);

        if (begin_transcation_list.size() != commit_list.size())
          rewrite_log(filename, lines, last_cmd);

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
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
        
        Session session = new Session(-1);
        session.setDatabase(database);
        read_log(session);
        
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
      for (Column column : table.getColumns()) {
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

  public int insert(String tableName, List<String> columnNames, List<List<Value>> values, Session session) throws IOException {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);

    long session_id = session.getSessionId();
    if (transactionManager.contain_session(session_id))
    {
      transactionManager.wait_for_write(session_id, table);
      table.insert(columnNames, values);

      return 1;
    }
    else
      return -1;
  }

  public ArrayList<Table> getTables(List<TableQuery> tableQueries, Session session) {
    Database database = session.getDatabase();

    try {
      database.lock.readLock().lock();

      int size = tableQueries.size();
      ArrayList<Table> tables = new ArrayList<>();
      ArrayList<String> table_names = new ArrayList<>();
      for (int i = 0; i < size; i++)
      {
        TableQuery tableQuery = tableQueries.get(i);
        table_names.add(tableQuery.getTableLeft());
        if (tableQuery.getTableRight() != null)
          table_names.add(tableQuery.getTableRight());
      }

      ArrayList<String> new_table_names = new ArrayList<String>(new HashSet<String>(table_names));
      int new_size = new_table_names.size();
      for (int i = 0; i < size; i++)
        tables.add(database.getTable(new_table_names.get(i)));

      return tables;
    } finally {
      database.lock.readLock().unlock();
    }
  }

  public QueryResult select(List<String> columnNames, List<TableQuery> tableQueries, Where condition, boolean distinct, Session session) {
    Database database = session.getDatabase();
    List<QueryTable> queryTables = tableQueries.stream().map(tableQuery -> getQueryTable(tableQuery, session)).collect(Collectors.toList());

    long session_id = session.getSessionId();
    if (transactionManager.contain_session(session_id))
    {
      transactionManager.wait_for_read(session_id, getTables(tableQueries, session));

      return database.select(queryTables, columnNames, condition, distinct);
    }
    else
      return null;
  }

  public int delete(String tableName, Where condition, Session session) throws IOException {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);

    long session_id = session.getSessionId();
    if (transactionManager.contain_session(session_id))
    {
      transactionManager.wait_for_write(session_id, table);
      return table.delete(condition);
    }
    else
      return -1;
  }

  public int update(String tableName, String columnName, Expression expression, Where condition, Session session) throws IOException {
    Database database = session.getDatabase();
    Table table = database.getTable(tableName);

    long session_id = session.getSessionId();
    if (transactionManager.contain_session(session_id))
    {
      transactionManager.wait_for_write(session_id, table);
      return table.update(columnName, expression, condition);
    }
    else
      return -1;
  }

  public boolean begin_transaction(Session session) {
    long session_id = session.getSessionId();
    if (!transactionManager.contain_session(session_id))
    {
      transactionManager.add_session(session_id);
      transactionManager.put_S_lock(session_id, new ArrayList<>());
      transactionManager.put_X_lock(session_id, new ArrayList<>());

      return true;
    }
    else
      return false;
  }

  public boolean commit(Session session) {
    long session_id = session.getSessionId();
    if (transactionManager.contain_session(session_id))
    {
      Database database = session.getDatabase();
      String db_name = database.getName();
      transactionManager.remove_session(session_id);

      ArrayList<String> table_list = transactionManager.get_X_lock(session_id);
      for (String table_name : table_list) {
        database.commitTable(table_name);
        database.getTable(table_name).free_X_lock(session_id);
      }
      table_list.clear();
      transactionManager.put_X_lock(session_id, table_list);

      return true;
    }
    else
      return false;
  }

  public List<String> getTables(String databaseName, Session session) {
    if (databaseName == null) {
      databaseName = session.getDatabaseName();
    }
    Database database = databases.get(databaseName);
    return Arrays.asList(database.getTables().keySet().toArray(new String[0]));
  }

  public List<String> getDatabases(Session session) {
    return Arrays.asList(databases.keySet().toArray(new String[0]));
  }

  public static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
