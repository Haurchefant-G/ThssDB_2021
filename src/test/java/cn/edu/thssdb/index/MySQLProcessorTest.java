package cn.edu.thssdb.index;

import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.server.Session;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ValueType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class MySQLProcessorTest {
    private static final ThssDB thssDB = ThssDB.getInstance();
    private static final Manager manger = Manager.getInstance();
    private static final SQLProcessor sqlProcessor = new SQLProcessor(manger);
    private static final long sessionId = thssDB.setupSession();
    private static final Session session = manger.getSession(sessionId);

    @Before
    public void init() {
        manger.createDatabaseIfNotExists("test");
        manger.createDatabaseIfNotExists("test2");
        manger.switchDatabase("test", session);
    }

    /*
    create_db_stmt :
        K_CREATE K_DATABASE database_name ;
     */
    @Test
    public void testCreateDb() {
        String sql = "create database newdb;";
        try {
            manger.deleteDatabase("newdb", session);
        } catch (Exception ignored) {

        }

        CreateDatabaseStatement statement = (CreateDatabaseStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.CREATE_DATABASE;
        assert statement.getDatabaseName().equals("newdb");
        assert manger.getDatabase("newdb") == null;
        SQLResult sqlResult = sqlProcessor.createDatabase(statement, session);
        assert sqlResult.isSucceed();
        assert manger.getDatabase("newdb") != null;
    }

    /*
    create_user_stmt :
        K_CREATE K_USER user_name K_IDENTIFIED K_BY password ;
     */
    @Test
    public void testCreateUser() {
        String sql = "create user liqi identified by '123456';";
        CreateUserStatement statement = (CreateUserStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.CREATE_USER;
        assert statement.getUsername().equals("liqi");
        assert statement.getPassword().equals("'123456'");
        // TODO:
    }

    /*
    drop_db_stmt :
        K_DROP K_DATABASE ( K_IF K_EXISTS )? database_name ;
     */
    @Test
    public void testDropDb() {
        String sql = "drop database test;";

        DropDatabaseStatement statement = (DropDatabaseStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DROP_DATABASE;
        assert statement.getDatabaseName().equals("test");
        assert manger.getDatabase("test") != null;
        SQLResult sqlResult = sqlProcessor.dropDatabase(statement, session);
        assert sqlResult.isSucceed();
        assert manger.getDatabase("test") == null;
    }

    /*
    drop_user_stmt :
        K_DROP K_USER ( K_IF K_EXISTS )? user_name ;
     */
    @Test
    public void testDropUser() {
        String sql = "drop user liqi;";

        DropUserStatement statement = (DropUserStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DROP_USER;
        assert statement.getUserName().equals("liqi");
    }

    /*
    use_db_stmt :
        K_USE database_name;
     */
    @Test
    public void testUseDb() {
        Session session = manger.getSession(sessionId);
        String sql = "use test2;";

        UseDatabaseStatement statement = (UseDatabaseStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.USE_DATABASE;
        assert statement.getDatabaseName().equals("test2");

        assert session.getDatabaseName().equals("test");
        SQLResult sqlResult = sqlProcessor.useDatabase(statement, session);
        assert sqlResult.isSucceed();
        assert session.getDatabaseName().equals("test2");
    }

    /*
    drop_table_stmt :
        K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Test
    public void testDropTable() {
        testCreateTable();

        String sql = "drop table person;";
        DropTableStatement statement = (DropTableStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DROP_TABLE;
        assert statement.getTableName().equals("person");

        assert manger.getDatabase("test").getTables().containsKey("person");
        SQLResult sqlResult = sqlProcessor.dropTable(statement, session);
        assert sqlResult.isSucceed();
        assert !manger.getDatabase("test").getTables().containsKey("person");
    }

    /*
    create_table_stmt :
    K_CREATE K_TABLE table_name
        '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Test
    public void testCreateTable() {
        String sql = "CREATE TABLE person (name String(256) PRIMARY KEY, age Int not null);";
        CreateTableStatement statement = (CreateTableStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.CREATE_TABLE;
        assert statement.getTableName().equals("person");
        for (Column column : statement.getColumnList()) {
            if (column.getName().equals("age")) {
                assert column.getType() == ColumnType.INT;
                assert column.isNotNull();
            } else if (column.getName().equals("name")) {
                assert column.getType() == ColumnType.STRING;
                assert column.getMaxLength() == 256;
                assert column.isNotNull();
                assert column.isPrimary();
            }
        }

        try {
            manger.getDatabase("test").drop("person");
        } catch (Exception ignored) {

        }

        assert !manger.getDatabase("test").getTables().containsKey("person");
        SQLResult sqlResult = sqlProcessor.createTable(statement, session);
        assert sqlResult.isSucceed();
        assert manger.getDatabase("test").getTables().containsKey("person");
    }

    /*
    insert_stmt :
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Test
    public void testInsert() {
        testCreateTable();

        String sql = "insert into person (name, age) values ('liqi', 20), ('guiacan', 21);";
        InsertStatement statement = (InsertStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.INSERT;
        assert statement.getTableName().equals("person");
        assert statement.getColumnNames().get(0).equals("name");
        assert statement.getColumnNames().get(1).equals("age");
        assert statement.getValues().get(0).get(0).getValue().equals("'liqi'");
        assert ((Number)statement.getValues().get(0).get(1).getValue()).intValue() == 20;
        assert statement.getValues().get(1).get(0).getValue().equals("'guiacan'");
        assert ((Number)statement.getValues().get(1).get(1).getValue()).intValue() == 21;

        SQLResult sqlResult = sqlProcessor.insert(statement, session);
        assert sqlResult.isSucceed();
        Table table = manger.getDatabase("test").getTable("person");

        assert table.hasKey(new Entry("'liqi'"));
        assert table.hasKey(new Entry("'guiacan'"));
    }

    /*
    update_stmt :
    K_UPDATE table_name
        K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
    */
    @Test
    public void testUpdate() {
        testInsert();

        String sql = "update person set name = 'qiqi' where age=20";
        UpdateStatement statement = (UpdateStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.UPDATE;
        assert statement.getTableName().equals("person");
        assert statement.getExpression().getValue().getValue().equals("'qiqi'");

        SQLResult sqlResult = sqlProcessor.update(statement, session);
        assert sqlResult.isSucceed();
        Table table = manger.getDatabase("test").getTable("person");
        assert table.hasKey(new Entry("'qiqi'"));
    }

    /*
    delete_stmt :
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testDelete() {
        testInsert();

        String sql = "delete from person where name='liqi'";
        DeleteStatement statement = (DeleteStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DELETE;
        assert statement.getTableName().equals("person");
        assert statement.getWhere().getCondition().getLeft().getValue().getValue().equals("name");
        assert statement.getWhere().getCondition().getRight().getValue().getValue().equals("'liqi'");

        Table table = manger.getDatabase("test").getTable("person");
        assert table.hasKey(new Entry("'liqi'"));
        SQLResult sqlResult = sqlProcessor.delete(statement, session);
        assert sqlResult.isSucceed();
        assert !table.hasKey(new Entry("'liqi'"));
    }

    /*
    show_tables_stmt :
        K_SHOW K_TABLES;
     */
    @Test
    public void testShowTables() {
        createSchema();

        String sql = "show tables;";
        ShowTablesStatement statement = (ShowTablesStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.SHOW_TABLES;

        SQLResult sqlResult = sqlProcessor.showTables(statement, session);
        assert sqlResult.isSucceed();
        assert sqlResult.getRowList().get(0).get(0).equals("person");
        assert sqlResult.getRowList().get(1).get(0).equals("book");
    }


    @Test
    public void createSchema() {
        String sql = "CREATE TABLE person (name String(256) PRIMARY KEY, age Int not null); "
                   + "CREATE TABLE book (id int, name String(256), owner String(256) not null, primary key(id)); "
                   + "insert into person (name, age) values ('liqi', 20), ('guiacan', 21); "
                   + "INSERT INTO book values (1, 'math', 'liqi'), (2, 'chinese', 'guiacan'), (3, 'english', 'liqi'), (4, 'english', 'guiacan')";

        try {
            manger.getDatabase("test").drop("person");
        } catch (Exception ignored) {

        }

        try {
            manger.getDatabase("test").drop("book");
        } catch (Exception ignored) {

        }

        List<Statement> statement = sqlProcessor.parseSQL(sql);
        List<SQLResult> sqlResults = sqlProcessor.executeSQL(statement, session);
        for (SQLResult sqlResult : sqlResults) {
            assert sqlResult.isSucceed();
        }
    }

    /*
    show_meta_stmt :
        K_SHOW K_TABLE table_name ;
     */
    @Test
    public void testShowMeta() {
        createSchema();

        String sql = "show table person;";
        ShowTableMetaStatement statement = (ShowTableMetaStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.SHOW_TABLE_META;
        assert statement.getTableName().equals("person");

        SQLResult sqlResult = sqlProcessor.showTableMeta(statement, session);
        assert sqlResult.isSucceed();
        assert sqlResult.getRowList().get(0).get(0).equals("name");
        assert sqlResult.getRowList().get(1).get(0).equals("age");
    }


    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testSelect() {
        createSchema();

        String sql = "select person.name, book.name from person join book on person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select age from person join book on person.name=book.owner;";
//        String sql = "select name, age from person;";

        SelectStatement statement = (SelectStatement) sqlProcessor.parseSQL(sql).get(0);

        SQLResult sqlResult = sqlProcessor.select(statement, session);
        assert sqlResult.isSucceed();
        assert sqlResult.getColumnList().get(0).equals("person.name");
        assert sqlResult.getColumnList().get(1).equals("book.name");
        assert sqlResult.getRowList().get(0).get(0).equals("'liqi'");
        assert sqlResult.getRowList().get(0).get(1).equals("'english'");
    }
}
