package cn.edu.thssdb.utils;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class TableMetaVisitorTest {

    @Test
    public void test1() {
        String line = "CREATE TABLE person(name STRING(256),age INT NOT NULL, PRIMARY KEY(name));";
        TableMetaVisitor tableMeta = new TableMetaVisitor();
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        MetaInfo meta = tableMeta.visitCreate_table_stmt(parser.create_table_stmt());

        assert meta.getTableName().equals("person");

        List<Column> columns = meta.getColumns();
        assert columns.size() == 2;

        assert columns.get(0).getName().equals("name");
        assert columns.get(0).getMaxLength() == 256;
        assert columns.get(0).isPrimary() == true;
        assert columns.get(0).isNotNull() == true;

        assert columns.get(1).getName().equals("age");
        assert columns.get(1).getMaxLength() == 4;
        assert columns.get(1).isPrimary() == false;
        assert columns.get(1).isNotNull() == true;
    }

    @Test
    public void test2() {
        String line = "CREATE TABLE book(id INT,name STRING(256),owner STRING(256) NOT NULL, price DOUBLE, PRIMARY KEY(id));";
        TableMetaVisitor tableMeta = new TableMetaVisitor();
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        MetaInfo meta = tableMeta.visitCreate_table_stmt(parser.create_table_stmt());

        assert meta.getTableName().equals("book");

        List<Column> columns = meta.getColumns();
        assert columns.size() == 4;

        assert columns.get(0).getName().equals("id");
        assert columns.get(0).getMaxLength() == 4;
        assert columns.get(0).isPrimary() == true;
        assert columns.get(0).isNotNull() == true;

        assert columns.get(1).getName().equals("name");
        assert columns.get(1).getMaxLength() == 256;
        assert columns.get(1).isPrimary() == false;
        assert columns.get(1).isNotNull() == false;

        assert columns.get(2).getName().equals("owner");
        assert columns.get(2).getMaxLength() == 256;
        assert columns.get(2).isPrimary() == false;
        assert columns.get(2).isNotNull() == true;

        assert columns.get(3).getName().equals("price");
        assert columns.get(3).getMaxLength() == 8;
        assert columns.get(3).isPrimary() == false;
        assert columns.get(3).isNotNull() == false;

    }
}
