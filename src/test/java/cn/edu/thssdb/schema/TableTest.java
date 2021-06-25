package cn.edu.thssdb.schema;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.query.MetaInfo;
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
public class TableTest {
    static ArrayList<Column> columns;
    static Row row;
    static String tableName;
    static Table table;
    static String databaseName = "test";
    private static final SQLProcessor sqlProcessor = new SQLProcessor(null);

    @BeforeClass
    public static void setup() throws NoSuchFieldException, IllegalAccessException {
        String line = "CREATE TABLE book(id INT,name STRING(256),owner STRING(256) NOT NULL, price DOUBLE, PRIMARY KEY(id));";
        TableMetaVisitor tableMeta = new TableMetaVisitor();
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        MetaInfo meta = tableMeta.visitCreate_table_stmt(parser.create_table_stmt());
        columns = meta.getColumns();
        tableName = meta.getTableName();

        File f = new File(tableName + ".table");
        if (f.exists())
            f.delete();
        f = new File(tableName + ".index");
        if (f.exists())
            f.delete();

        ArrayList<Entry> entryList = new ArrayList<>();
        entryList.add(new Entry(1));
        entryList.add(new Entry("name"));
        entryList.add(new Entry("owner"));
        entryList.add(new Entry(3.99));
        row = new Row(entryList);

        table = new Table(databaseName, tableName, columns);
    }

    @Test
    public void test1_insert() throws IOException {
        table.insert(row);
        assert table.hasKey(new Entry(1));
        assert table.getIndexSize() == 1;
        System.out.println(row.page);
        System.out.println(row.offset);
    }

    @Test
    public void test2_recover() throws IOException {
        table.close();
        table = new Table(databaseName, tableName, columns);
        assert table.hasKey(new Entry(1));
        assert table.getIndexSize() == 1;
        Row nrow = table.fileStorage.searchRowInPage(row.page, row.offset);
        assert row.getEntries().equals(nrow.getEntries());
    }

    @Test
    public void test3_delete() throws IOException {
        table.delete(row);
        assert table.getIndexSize() == 0;
        Row nrow = table.fileStorage.searchRowInPage(row.page, row.offset);
        assert !row.getEntries().equals(nrow.getEntries());
    }

    @Test
    public void test4_insert() throws IOException {
        table.insert(row);
        assert table.hasKey(new Entry(1));
        assert table.getIndexSize() == 1;
        System.out.println(row.page);
        System.out.println(row.offset);
    }

    @AfterClass()
    public static void destroy() {
        table.close();
    }

}
