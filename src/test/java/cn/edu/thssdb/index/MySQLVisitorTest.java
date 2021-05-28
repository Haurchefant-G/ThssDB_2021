package cn.edu.thssdb.index;

import cn.edu.thssdb.parser.SQLProcessor;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ValueType;
import org.junit.Test;

public class MySQLVisitorTest {
    private static final SQLProcessor sqlProcessor = new SQLProcessor(null);

    /*
    create_db_stmt :
        K_CREATE K_DATABASE database_name ;
     */
    @Test
    public void testCreateDb() {
        String sql = "create database test;";
        CreateDatabaseStatement statement = (CreateDatabaseStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.CREATE_DATABASE;
        assert statement.getDatabaseName().equals("test");
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
        String sql = "use test;";
        UseDatabaseStatement statement = (UseDatabaseStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.USE_DATABASE;
        assert statement.getDatabaseName().equals("test");
    }

    /*
    drop_table_stmt :
        K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Test
    public void testDropTable() {
        String sql = "drop table test;";
        DropTableStatement statement = (DropTableStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DROP_TABLE;
        assert statement.getTableName().equals("test");
    }

    /*
    show_tables_stmt :
        K_SHOW K_TABLES;
     */
    @Test
    public void testShowTables() {
        String sql = "show tables;";
        ShowTablesStatement statement = (ShowTablesStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.SHOW_TABLES;
    }

    /*
    show_meta_stmt :
        K_SHOW K_TABLE table_name ;
     */
    @Test
    public void testShowMeta() {
        String sql = "show table test;";
        ShowTableMetaStatement statement = (ShowTableMetaStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.SHOW_TABLE_META;
        assert statement.getTableName().equals("test");
    }

    /*
    create_table_stmt :
    K_CREATE K_TABLE table_name
        '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Test
    public void testCreateTable() {
        String sql = "CREATE TABLE person (name String(256) PRIMARY KEY, ID Int not null, PRIMARY KEY(ID));";
        CreateTableStatement statement = (CreateTableStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.CREATE_TABLE;
        assert statement.getTableName().equals("person");
        for (Column column : statement.getColumnList()) {
            if (column.getName().equals("id")) {
                assert column.getType() == ColumnType.INT;
                assert column.isPrimary();
                assert column.isNotNull();
            } else if (column.getName().equals("name")) {
                assert column.getType() == ColumnType.STRING;
                assert column.getMaxLength() == 256;
                assert column.isNotNull();
                assert column.isPrimary();
            }
        }
    }

    /*
    insert_stmt :
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Test
    public void testInsert() {
        String sql = "insert into person (name, id) values ('liqi', 1), ('guiacan', 2)";
        InsertStatement statement = (InsertStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.INSERT;
        assert statement.getTableName().equals("person");
        assert statement.getColumnNames().get(0).equals("name");
        assert statement.getColumnNames().get(1).equals("id");
        assert statement.getValues().get(0).get(0).getValue().equals("'liqi'");
        assert ((Number)statement.getValues().get(0).get(1).getValue()).intValue() == 1;
        assert statement.getValues().get(1).get(0).getValue().equals("'guiacan'");
        assert ((Number)statement.getValues().get(1).get(1).getValue()).intValue() == 2;

        String sql1 = "insert into person values ('liqi', 1), ('guiacan', 2)";
        InsertStatement statement1 = (InsertStatement) sqlProcessor.parseSQL(sql1).get(0);
        assert statement1.getColumnNames().size() == 0;
    }

    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testSelect() {
        String sql = "select person.name, book.name from person join book on person.name=book.owner where person.name='liqi' && book.name='english';";
        SelectStatement statement = (SelectStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.SELECT;
        assert statement.getColumnNames().get(0).equals("person.name");
        assert statement.getColumnNames().get(1).equals("book.name");
        assert statement.getTableQueries().get(0).getTableLeft().equals("person");
        assert statement.getTableQueries().get(0).getTableRight().equals("book");
        assert statement.getWhere().getRight().getCondition().getLeft().getValue().getType() == ValueType.COLUMN;
    }

    /*
    update_stmt :
    K_UPDATE table_name
        K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
    */
    @Test
    public void testUpdate() {
        String sql = "update person set name = 'liqi' where age>10";
        UpdateStatement statement = (UpdateStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.UPDATE;
        assert statement.getTableName().equals("person");
        assert statement.getExpression().getValue().getValue().equals("'liqi'");
    }

    /*
    delete_stmt :
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Test
    public void testDelete() {
        String sql = "delete from person where name='liqi'";
        DeleteStatement statement = (DeleteStatement) sqlProcessor.parseSQL(sql).get(0);
        assert statement.getType() == StatementType.DELETE;
        assert statement.getTableName().equals("person");
        assert statement.getWhere().getCondition().getLeft().getValue().getValue().equals("name");
        assert statement.getWhere().getCondition().getRight().getValue().getValue().equals("'liqi'");
    }


    public static void main(String[] args) {
    }
}
