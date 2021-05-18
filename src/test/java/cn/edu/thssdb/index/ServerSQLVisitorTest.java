package cn.edu.thssdb.index;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.utils.ServerSQLVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class ServerSQLVisitorTest {


    public static void main(String[] args) {

        ServerSQLVisitor ServerSql = new ServerSQLVisitor();
//        String line = "CREATE TABLE person (name String(256), ID Int not null,\n" +
//                "PRIMARY KEY(ID)); " +
//                "DROP TABLE tableName";
//        String line = "DELETE FROM table where 1 = 2 and 3 = 4 or 1 = 2 and 3 = 5";
        String line = "SELECT AttrName1, AttrName2, " +
                "AttrName1, AttrName2 FROM " +
                "tableName1 JOIN tableName2 ON tableName1.attrName1 = " +
                "tableName2.attrName2 WHERE attrName1 = attrValue || ( a=b && c = d);";
        CharStream stream = CharStreams.fromString(line);
        SQLLexer lexer = new SQLLexer(stream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(token);
        //SQLParser.ParseContext c = parser.parse();
//        ServerSql.visitParse(parser.parse());
        ServerSql.visitSql_stmt_list(parser.sql_stmt_list());
       // System.out.print(table);
        System.exit(0);
    }
}
