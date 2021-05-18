package cn.edu.thssdb.schema;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.utils.DatabaseMetaVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Database selectDatabase;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
    selectDatabase = null;

    recoverMeta();
    createDatabaseIfNotExists("Default");
    switchDatabase("Default");

  }

  /**
   * 创建新的数据库
   * @param name 数据库名
   */
  private void createDatabaseIfNotExists(String name) {
    // TODO
    lock.writeLock().lock();
    if (databases.get(name) == null) {
      Database database = new Database(name);
      databases.put(name, database);
    }
    persistMeta();
    lock.writeLock().unlock();
  }

  /**
   * 删除数据库，若删除的是当前操作数据库,应该将当前操作数据库置为null
   * @param name
   */
  private void deleteDatabase(String name) {
    // TODO
    lock.writeLock().lock();
    Database database = databases.remove(name);
    if (database == selectDatabase) {
      selectDatabase = null;
    }
    database.clear();
    database = null;
    persistMeta();
    lock.writeLock().lock();
  }

  /**
   * 切换当前操作的数据库，恢复其元数据，并关闭之前的数据库，持久化关闭的数据库的元数据
   * @param name 数据库名
   */
  public void switchDatabase(String name) {
    // TODO
    lock.writeLock().lock();
    Database newSelect = databases.getOrDefault(name, selectDatabase);
    if (newSelect != selectDatabase) {
      if (selectDatabase != null) {
        selectDatabase.quit();
      }
      selectDatabase = newSelect;
      selectDatabase.start();
    }
    lock.writeLock().unlock();
  }

  /**
   * 更新所有数据库元数据
   */
  private void persistMeta(){
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
  private void recoverMeta() {
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
    }
    lock.readLock().unlock();
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}