package cn.edu.thssdb.schema;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.utils.DatabaseMetaVisitor;
import cn.edu.thssdb.utils.TableMetaVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseTest {
    static Database database;
    static String dbname;
    static String tableName;
    static ArrayList<Column> columns;
    static Row row;
    static Table table;


    @BeforeClass
    public static void setup() {
        String line = "create database dbtest;";
        dbname = "dbtest";
        DatabaseMetaVisitor databaseMeta = new DatabaseMetaVisitor();
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);

        File f = new File(dbname + ".db");
        if (f.exists())
            f.delete();

        database = databaseMeta.visitCreate_db_stmt(parser.create_db_stmt());
        // database = new Database("123");
        assert database.getName().equals(dbname);
    }

//    @Test
//    public void test1_start() throws IOException {
//        database.start();
//        assert database.getSessionNum() == 0;
//        System.out.println(database.getTables());
//        assert database.getTables() == null;
//
//        String line = "CREATE TABLE book(id INT,name STRING(256),owner STRING(256) NOT NULL, price DOUBLE, PRIMARY KEY(id));";
//        TableMetaVisitor tableMeta = new TableMetaVisitor();
//        CharStream stream = CharStreams.fromString(line);
//        SQLLexer lexer = new SQLLexer(stream);
//        CommonTokenStream token = new CommonTokenStream(lexer);
//        SQLParser parser = new SQLParser(token);
//        MetaInfo meta = tableMeta.visitCreate_table_stmt(parser.create_table_stmt());
//        columns = meta.getColumns();
//        tableName = meta.getTableName();
//
//        database.create(tableName, columns);
//
//        assert database.getTables().size() == 1;
//        table = database.getTable(tableName);
//        assert table.getTableName() == tableName;
//        System.out.println(database.getColumns(tableName));
//
//        ArrayList<Entry> entryList = new ArrayList<>();
//        entryList.add(new Entry(1));
//        entryList.add(new Entry("name"));
//        entryList.add(new Entry("owner"));
//        entryList.add(new Entry(3.99));
//        row = new Row(entryList);
//        table.insert(row);
//    }
//
//    @Test
//    public void test2_recover() throws IOException {
//        database.quit();
//        database.start();
//        assert database.getTables().size() == 1;
//        table = database.getTable(tableName);
//        assert table.getTableName() == tableName;
//        System.out.println(database.getColumns(tableName));
//
//        Row nrow = table.fileStorage.searchRowInPage(0, table.fileStorage.getRowLen());
//        System.out.println(nrow);
//    }

    @AfterClass
    public static void destroy() {
        database.clear();
        assert database.getTables() == null;
    }
}
