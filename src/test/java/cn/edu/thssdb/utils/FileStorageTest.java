package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.RowNotExistException;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileStorageTest {
    static ArrayList<Column> columns;
    static FileStorage fileStorage;
    static Row row;

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

        File f = new File(".FileStorageTest");
        if (f.exists())
            f.delete();

        fileStorage = new FileStorage(".FileStorageTest", columns);

        ArrayList<Entry> entryList = new ArrayList<>();
        entryList.add(new Entry(1));
        entryList.add(new Entry("name"));
        entryList.add(new Entry("owner"));
        entryList.add(new Entry(3.99));
        row = new Row(entryList);

        Field filed = fileStorage.getClass().getDeclaredField("keeppages");
        filed.setAccessible(true);
        int keeppages = 2;
        filed.set(keeppages, 2);
        assert fileStorage.getPageNum() == 0;
    }

    @Test
    public void test1_transferBetweenRowAndBuf() {
        ByteBuffer buf = fileStorage.RowToByteBuffer(row);
        Row newrow = fileStorage.ByteBufferToRow(buf);
        assert row.getEntries().equals(newrow.getEntries());
    }

    @Test
    public void test2_openFile() throws IOException {
        fileStorage.openFile();
        assert fileStorage.file.length() == 0;
        fileStorage.closeFile();
    }

    @Test
    public void test3_insert() throws NoSuchFieldException, IllegalAccessException, IOException {

        fileStorage.insertToPage(row);
        assert fileStorage.getPageNum() == 1;
        assert fileStorage.getPageIndex(0) == 0;
    }

    @Test
    public void test4_searchExist() throws IOException {
        try {
            Row search = fileStorage.searchRowInPage(0, fileStorage.getRowLen());
            System.out.println(search.toString());
            assert search.getEntries().equals(row.getEntries());
        }
        catch (RowNotExistException e) {
            System.out.println(e.getMessage());
        }
        assert fileStorage.getPageNum() == 1;
        assert fileStorage.getPageIndex(0) == 0;
    }

    @Test
    public void test5_searchNotExist() throws IOException {
        try {
            Row search = fileStorage.searchRowInPage(1, fileStorage.getRowLen());
            System.out.println(search);
        }
        catch (RowNotExistException e) {
            System.out.println(e.getMessage());
        }
        assert fileStorage.getPageNum() == 1;
        assert fileStorage.getPageIndex(0) == 0;
    }

    @Test
    public void test6_deleteNotExist() throws IOException {
        fileStorage.deleteFromPage(0, fileStorage.getRowLen());
        try {
            Row search = fileStorage.searchRowInPage(1, fileStorage.getRowLen());
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void test6_insertBeyondPage() throws IOException {
        int i;
        for (i = 1; fileStorage.getPageNum() <= 1; ++i)
        {
            fileStorage.insertToPage(row.updateRow(0, new Entry(i)));
        }
        i = i - 1;
        assert fileStorage.getPageNum() == 2;
        assert fileStorage.getPageIndex(1) == 1;

        for (int j = 1; j < i; ++j)
        {
            try {
                Row search = fileStorage.searchRowInPage(0, j * fileStorage.getRowLen());
                System.out.println(search);
                assert search.getEntries().equals(row.updateRow(0, new Entry(j)).getEntries());
            }
            catch (RowNotExistException e) {
                System.out.println(e.getMessage());
            }
        }

        try {
            Row search = fileStorage.searchRowInPage(1, fileStorage.getRowLen());
            System.out.println(search);
            assert search.getEntries().equals(row.updateRow(0, new Entry(i)).getEntries());
        }
        catch (RowNotExistException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void test7_Update() throws IOException {
        int num = 20;
        fileStorage.insertToPage(row.updateRow(0, new Entry(1234)));
        for (int i = 0; i <= num; ++i)
        {
            fileStorage.updateRowInPage(1, fileStorage.getRowLen(), row.updateRow(0, new Entry(i)));
        }
        try {
            Row search = fileStorage.searchRowInPage(1, fileStorage.getRowLen());
            System.out.println(search);
            assert search.getEntries().equals(row.updateRow(0, new Entry(num)).getEntries());
            search = fileStorage.searchRowInPage(1, 2 * fileStorage.getRowLen());
            System.out.println(search);
            assert search.getEntries().equals(row.updateRow(0, new Entry(1234)).getEntries());
        }
        catch (RowNotExistException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void test7_clearUp() throws IOException {
        fileStorage.clearUp();
        assert fileStorage.getPageNum() == 1;
        assert fileStorage.getPageIndex(0) == 1;
    }

    @Test
    public void test8_deleteExist() throws IOException {
        int index = fileStorage.searchRowIndexInPage(0, 0);
        System.out.println(index);
        assert index == -1;

        fileStorage.deleteFromPage(0, 3 * fileStorage.getRowLen());
        fileStorage.deleteFromPage(0, 5 * fileStorage.getRowLen());
        try {
            index = fileStorage.searchRowIndexInPage(0, 3 * fileStorage.getRowLen());
            System.out.println(index);
            assert index == -1;

            index = fileStorage.searchRowIndexInPage(0, 5 * fileStorage.getRowLen());
            System.out.println(index);
            assert index == 3 * fileStorage.getRowLen();

            index = fileStorage.searchRowIndexInPage(0, 0);
            System.out.println(index);
            assert index == 5 * fileStorage.getRowLen();
        }
        catch (RowNotExistException e) {
            System.out.println(e.getMessage());
        }
    }


    @AfterClass
    public static void destroy() {
        fileStorage.close();
        assert fileStorage.getPageNum() == 0;
        File f = new File(".FileStorageTest");
        if (f.exists())
            f.delete();
    }

}
