package cn.edu.thssdb.index;

import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.server.Session;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.type.ColumnType;
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

    /**
     begin_transaction_stmt :
        K_BEGIN K_TRANSACTION;
    */
    public void testBegin_transaction() {
        String sql = "begin transaction;";
        BeginTransactionStatement statement = (BeginTransactionStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.BEGIN_TRANSACTION;

        Integer size1 = manger.transactionManager.get_session_size();
        SQLResult sqlResult = sqlProcessor.begin_transaction(session);
        assert sqlResult.isSucceed();

        Integer size2 = manger.transactionManager.get_session_size();
        assert (size2-size1 == 1);
    }

    /**
     commit_stmt:
     K_COMMIT ;
     */
    public void testCommit() {
        String sql = "commit;";
        CommitStatement statement = (CommitStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.COMMIT;

        Integer size1 = manger.transactionManager.get_session_size();
        SQLResult sqlResult = sqlProcessor.commit(session);
        assert sqlResult.isSucceed();

        Integer size2 = manger.transactionManager.get_session_size();
        assert (size1-size2 == 1);
    }

    /*
    insert_stmt :
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Test
    public void testInsert() {
        testCreateTable();
        testBegin_transaction();

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

        testCommit();
    }

    /*
    update_stmt :
    K_UPDATE table_name
        K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
    */
    @Test
    public void testUpdate() {
        testInsert();
        testBegin_transaction();

        String sql = "update person set name = 'qiqi' where age=20";
        UpdateStatement statement = (UpdateStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.UPDATE;
        assert statement.getTableName().equals("person");
        assert statement.getExpression().getValue().getValue().equals("'qiqi'");

        SQLResult sqlResult = sqlProcessor.update(statement, session);
        assert sqlResult.isSucceed();
        Table table = manger.getDatabase("test").getTable("person");
        assert table.hasKey(new Entry("'qiqi'"));

        testCommit();
    }

    /*
    delete_stmt :
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testDelete() {
        testInsert();
        testBegin_transaction();

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

        testCommit();
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
        testBegin_transaction();

        String sql = "CREATE TABLE person (name String(256) PRIMARY KEY, age Int not null); "
                   + "CREATE TABLE book (id int, name String(256), owner String(256) not null, primary key(id)); "
                   + "CREATE TABLE live (name string(256) primary key, house string(256) not null);"
                   + "insert into person (name, age) values ('liqi', 20), ('guiacan', 21), ('sb', 32); "
                   + "insert into live (name, house) values ('liqi', 'datong'), ('guiacan', 'beijing'); "
                   + "INSERT INTO book values (1, 'math', 'liqi'), (2, 'chinese', 'guiacan'), (3, 'english', 'liqi'), (4, 'english', 'guiacan'), (5, 'english', 'sa')";

        try {
            manger.getDatabase("test").drop("person");
        } catch (Exception ignored) {

        }

        try {
            manger.getDatabase("test").drop("book");
        } catch (Exception ignored) {

        }

        try {
            manger.getDatabase("test").drop("live");
        } catch (Exception ignored) {

        }

        List<Statement> statement = sqlProcessor.parseSQL(sql);
        List<SQLResult> sqlResults = sqlProcessor.executeSQL(statement, session);
        for (SQLResult sqlResult : sqlResults) {
            assert sqlResult.isSucceed();
        }

        testCommit();
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
        testBegin_transaction();

//        String sql = "select * from person, book, live where person.name=live.name && person.name=book.owner;";
//        String sql = "select person.name, book.name from person, book where person.name=book.owner && person.name='liqi' && book.name='english';";
        String sql = "select person.name, book.name from person join book on person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select person.name, book.name from person left outer join book on person.name=book.owner;";
//        String sql = "select person.name, book.name from person join book on person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select * from person join book on person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select person.name, book.name from person, book where person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select * from person, book where person.name=book.owner where person.name='liqi' && book.name='english';";
//        String sql = "select age from person join book on person.name=book.owner;";
//        String sql = "select name, age from person;";

        SelectStatement statement = (SelectStatement) sqlProcessor.parseSQL(sql).get(0);

        SQLResult sqlResult = sqlProcessor.select(statement, session);
        assert sqlResult.isSucceed();
        assert sqlResult.getColumnList().get(0).equals("person.name");
        assert sqlResult.getColumnList().get(1).equals("book.name");
        assert sqlResult.getRowList().get(0).get(0).equals("'liqi'");
        assert sqlResult.getRowList().get(0).get(1).equals("'english'");

        testCommit();
    }

    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testMultiSelect() {
        createSchema();
        testBegin_transaction();

        String sql = "select * from person, book, live where person.name=live.name && person.name=book.owner;";
        SelectStatement statement = (SelectStatement) sqlProcessor.parseSQL(sql).get(0);

        SQLResult sqlResult = sqlProcessor.select(statement, session);
        assert sqlResult.isSucceed();
        assert sqlResult.getRowList().get(0).size() == 7;

        testCommit();
    }

    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testSelectOuterJoin() {
        createSchema();
        testBegin_transaction();

        String sql1 = "select person.name, book.name from person left outer join book on person.name=book.owner;";
        SelectStatement statement1 = (SelectStatement) sqlProcessor.parseSQL(sql1).get(0);
        SQLResult sqlResult1 = sqlProcessor.select(statement1, session);
        assert sqlResult1.isSucceed();
        assert sqlResult1.getRowList().get(sqlResult1.getRowList().size()-1).get(1).equals("null");

        String sql2 = "select person.name, book.name from person right outer join book on person.name=book.owner;";
        SelectStatement statement2 = (SelectStatement) sqlProcessor.parseSQL(sql2).get(0);
        SQLResult sqlResult2 = sqlProcessor.select(statement2, session);
        assert sqlResult2.isSucceed();
        assert sqlResult2.getRowList().get(sqlResult2.getRowList().size()-1).get(0).equals("null");

        String sql3 = "select person.name, book.name from person outer join book on person.name=book.owner;";
        SelectStatement statement3 = (SelectStatement) sqlProcessor.parseSQL(sql3).get(0);
        SQLResult sqlResult3 = sqlProcessor.select(statement3, session);
        assert sqlResult3.isSucceed();
        assert sqlResult3.getRowList().get(sqlResult3.getRowList().size()-2).get(1).equals("null");
        assert sqlResult3.getRowList().get(sqlResult3.getRowList().size()-1).get(0).equals("null");

        testCommit();
    }

    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testDistinctSelect() {
        createSchema();
        testBegin_transaction();

        String sql = "select distinct person.name from person, book, live where person.name=live.name && person.name=book.owner;";
        SelectStatement statement = (SelectStatement) sqlProcessor.parseSQL(sql).get(0);

        SQLResult sqlResult = sqlProcessor.select(statement, session);
        assert sqlResult.getRowList().size() == 2;

        testCommit();
    }
}
