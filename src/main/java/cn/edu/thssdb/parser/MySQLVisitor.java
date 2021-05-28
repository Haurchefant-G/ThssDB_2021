package cn.edu.thssdb.parser;

import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class MySQLVisitor extends SQLBaseVisitor<Object> {
    public MySQLVisitor() {
        super();
    }

    /*
    parse :
        sql_stmt_list ;
     */
    @Override
    public List<Statement> visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    /*
    sql_stmt_list :
        ';'* sql_stmt ( ';'+ sql_stmt )* ';'* ;
    */
    @Override
    public List<Statement> visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        List<Statement> statements = new ArrayList<>();
        for (SQLParser.Sql_stmtContext sql_stmtContext : ctx.sql_stmt()) {
            statements.add(visitSql_stmt(sql_stmtContext));
        }
        return statements;
    }

    /*
    sql_stmt :
        create_table_stmt
        | create_db_stmt
        | create_user_stmt
        | drop_db_stmt
        | drop_user_stmt
        | delete_stmt
        | drop_table_stmt
        | insert_stmt
        | select_stmt
        | create_view_stmt
        | drop_view_stmt
        | grant_stmt
        | revoke_stmt
        | use_db_stmt
        | show_db_stmt
        | show_table_stmt
        | show_meta_stmt
        | quit_stmt
        | update_stmt ;
     */
    @Override
    public Statement visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        return (Statement) visit(ctx.getChild(0));
    }

    /*
    create_db_stmt :
        K_CREATE K_DATABASE database_name ;
     */
    @Override
    public CreateDatabaseStatement visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String databaseName = visitDatabase_name(ctx.database_name());
        return new CreateDatabaseStatement(databaseName);
    }

    /*
    drop_db_stmt :
        K_DROP K_DATABASE ( K_IF K_EXISTS )? database_name ;
     */
    @Override
    public DropDatabaseStatement visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String databaseName = visitDatabase_name(ctx.database_name());
        return new DropDatabaseStatement(databaseName);
    }

    /*
    create_user_stmt :
        K_CREATE K_USER user_name K_IDENTIFIED K_BY password ;
     */
    @Override
    public CreateUserStatement visitCreate_user_stmt(SQLParser.Create_user_stmtContext ctx) {
        String username = visitUser_name(ctx.user_name());
        String password = visitPassword(ctx.password());
        return new CreateUserStatement(username, password);
    }

    /*
    drop_user_stmt :
        K_DROP K_USER ( K_IF K_EXISTS )? user_name ;
     */
    @Override
    public DropUserStatement visitDrop_user_stmt(SQLParser.Drop_user_stmtContext ctx) {
        String username = visitUser_name(ctx.user_name());
        return new DropUserStatement(username);
    }

    /*
    create_table_stmt :
        K_CREATE K_TABLE table_name
        '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Override
    public CreateTableStatement visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());
        ArrayList<Column> columns = new ArrayList<>();
        for (SQLParser.Column_defContext def : ctx.column_def()) {
            columns.add(visitColumn_def(def));
        }

        if (ctx.table_constraint() != null) {
            List<String> primaryKeys = visitTable_constraint(ctx.table_constraint());
            for (String primaryKey : primaryKeys) {
                for (Column column : columns) {
                    if (column.getName().equals(primaryKey)) {
                        column.setPrimary(1);
                        column.setNotNull(true);
                        break;
                    }
                }
            }
        }

        return new CreateTableStatement(tableName, columns);
    }


    /*
    show_tables_stmt :
        K_SHOW K_TABLES;
     */
    @Override
    public Object visitShow_tables_stmt(SQLParser.Show_tables_stmtContext ctx) {
        return new ShowTablesStatement();
    }

    /*
    show_table_stmt :
        K_SHOW K_TABLE table_name ;
     */
    @Override
    public ShowTableMetaStatement visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        String tableName = visitTable_name(ctx.table_name());
        return new ShowTableMetaStatement(tableName);
    }

    @Override
    public Object visitGrant_stmt(SQLParser.Grant_stmtContext ctx) {
        return super.visitGrant_stmt(ctx);
    }

    @Override
    public Object visitRevoke_stmt(SQLParser.Revoke_stmtContext ctx) {
        return super.visitRevoke_stmt(ctx);
    }

    /*
    use_db_stmt :
        K_USE database_name;
     */
    @Override
    public Object visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String databaseName = visitDatabase_name(ctx.database_name());
        return new UseDatabaseStatement(databaseName);
    }

    /*
    delete_stmt :
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Override
    public DeleteStatement visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = visitTable_name(ctx.table_name());
        Where condition = null;
        if (ctx.multiple_condition() != null) {
            condition = visitMultiple_condition(ctx.multiple_condition());
        }
        return new DeleteStatement(tableName, condition);
    }

    /*
    drop_table_stmt :
        K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Override
    public DropTableStatement visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = visitTable_name(ctx.table_name());
        return new DropTableStatement(tableName);
    }

    /*
    show_db_stmt :
        K_SHOW K_DATABASES;
     */
    @Override
    public ShowDatabasesStatement visitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {
        return new ShowDatabasesStatement();
    }

    @Override
    public Object visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        return super.visitQuit_stmt(ctx);
    }

    /*
    insert_stmt :
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Override
    public InsertStatement visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = visitTable_name(ctx.table_name());

        List<String> columnNames = new ArrayList<>();
        if (ctx.column_name() != null) {
            for (SQLParser.Column_nameContext columnNameContext : ctx.column_name()) {
                columnNames.add(visitColumn_name(columnNameContext));
            }
        }

        List<List<Value>> valueList = new ArrayList<>();
        for (SQLParser.Value_entryContext valueEntryContext : ctx.value_entry()) {
            List<Value> values = visitValue_entry(valueEntryContext);
            valueList.add(values);
        }

        return new InsertStatement(tableName, columnNames, valueList);
    }

    /*
    value_entry :
        '(' literal_value ( ',' literal_value )* ')' ;
     */
    @Override
    public List<Value> visitValue_entry(SQLParser.Value_entryContext ctx) {
        List<Value> values = new ArrayList<>();
        for (SQLParser.Literal_valueContext literalValueContext : ctx.literal_value()) {
            values.add(visitLiteral_value(literalValueContext));
        }
        return values;
    }

    /*
    select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
            K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Override
    public SelectStatement visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        boolean distinct = ctx.K_DISTINCT() != null;
        List<String> columns = new ArrayList<>();
        for (SQLParser.Result_columnContext resultColumnContext : ctx.result_column()) {
//            columns.add(visitResult_column(resultColumnContext));
            String columnName = resultColumnContext.getText().toLowerCase();
            if (columnName.equals("*")) {
                columns = null;
                break;
            }
            columns.add(columnName);
        }
        List<TableQuery> tableQueries = new ArrayList<>();
        for (SQLParser.Table_queryContext tableQueryContext : ctx.table_query()) {
            tableQueries.add(this.visitTable_query(tableQueryContext));
        }
        Where where = null;
        if (ctx.multiple_condition() != null) {
            where = visitMultiple_condition(ctx.multiple_condition());
        }
        return new SelectStatement(columns, tableQueries, where, distinct);
    }

    @Override
    public Object visitCreate_view_stmt(SQLParser.Create_view_stmtContext ctx) {
        return super.visitCreate_view_stmt(ctx);
    }

    @Override
    public Object visitDrop_view_stmt(SQLParser.Drop_view_stmtContext ctx) {
        return super.visitDrop_view_stmt(ctx);
    }

    /*
    update_stmt :
        K_UPDATE table_name
            K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = visitTable_name(ctx.table_name());
        String columnName = visitColumn_name(ctx.column_name());
        Expression expression = visitExpression(ctx.expression());
        Where condition = null;
        if (ctx.multiple_condition() != null) {
            condition = visitMultiple_condition(ctx.multiple_condition());
        }
        return new UpdateStatement(tableName, columnName, expression, condition);
    }

    /*
    column_def :
        column_name type_name column_constraint* ;
     */
    @Override
    public Column visitColumn_def(SQLParser.Column_defContext ctx) {
        String columnName = visitColumn_name(ctx.column_name());
        ColumnTypeWithLength columnType = visitType_name(ctx.type_name());
        List<ColumnConstraint> constraints = new ArrayList<>();
        for (SQLParser.Column_constraintContext columnConstraintContext : ctx.column_constraint()) {
            constraints.add(visitColumn_constraint(columnConstraintContext));
        }
        return new Column(columnName, columnType, constraints);
    }

    /*
    type_name :
        T_INT
        | T_LONG
        | T_FLOAT
        | T_DOUBLE
        | T_STRING '(' NUMERIC_LITERAL ')' ;
     */
    @Override
    public ColumnTypeWithLength visitType_name(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) {
            return new ColumnTypeWithLength(ColumnType.INT);
        } else if (ctx.T_LONG() != null) {
            return new ColumnTypeWithLength(ColumnType.LONG);
        } else if (ctx.T_FLOAT() != null) {
            return new ColumnTypeWithLength(ColumnType.FLOAT);
        } else if (ctx.T_DOUBLE() != null) {
            return new ColumnTypeWithLength(ColumnType.DOUBLE);
        } else {
            int num = Integer.parseInt(ctx.NUMERIC_LITERAL().getText());
            return new ColumnTypeWithLength(ColumnType.STRING, num);
        }
    }

    /*
    column_constraint :
        K_PRIMARY K_KEY
        | K_NOT K_NULL ;
     */
    @Override
    public ColumnConstraint visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.K_PRIMARY() != null) {
            return ColumnConstraint.PRIMARY;
        } else {
            return ColumnConstraint.NOTNULL;
        }
    }

    /*
    multiple_condition :
        '(' multiple_condition ')'
        | multiple_condition AND multiple_condition
        | multiple_condition OR multiple_condition
        | condition;
     */
    @Override
    public Where visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        if (ctx.condition() != null) {
            return new Where(visitCondition(ctx.condition()));
        } else {
            Where left = visitMultiple_condition(ctx.multiple_condition(0));
            Where right = visitMultiple_condition(ctx.multiple_condition(1));
            if (ctx.AND() != null) {
                return new Where(left, right, ConditionOp.AND);
            } else {
                return new Where(left, right, ConditionOp.OR);
            }
        }
    }

    /*
    condition :
        expression comparator expression;
     */
    @Override
    public Condition visitCondition(SQLParser.ConditionContext ctx) {
        Expression left = visitExpression(ctx.expression(0));
        Expression right = visitExpression(ctx.expression(1));
        Comparator comparator = visitComparator(ctx.comparator());
        return new Condition(left, right, comparator);
    }

    /*
    comparer :
        column_full_name
        | literal_value ;
     */
    @Override
    public Value visitComparer(SQLParser.ComparerContext ctx) {
        if (ctx.column_full_name() != null) {
            return new Value(ctx.column_full_name().getText().toLowerCase(), ValueType.COLUMN);
        } else {
            return visitLiteral_value(ctx.literal_value());
        }
    }

    /*
    comparator :
        EQ | NE | LE | GE | LT | GT ;
     */
    @Override
    public Comparator visitComparator(SQLParser.ComparatorContext ctx) {
        if (ctx.EQ() != null) {
            return Comparator.EQ;
        } else if (ctx.NE() != null) {
            return Comparator.NE;
        } else if (ctx.LE() != null) {
            return Comparator.LE;
        } else if (ctx.GE() != null) {
            return Comparator.GE;
        } else if (ctx.LT() != null) {
            return Comparator.LT;
        } else if (ctx.GT() != null) {
            return Comparator.GT;
        }
        return null;
    }

    /*
    expression :
        comparer
        | expression ( MUL | DIV ) expression
        | expression ( ADD | SUB ) expression
        | '(' expression ')';
     */
    @Override
    public Expression visitExpression(SQLParser.ExpressionContext ctx) {
        if (ctx.comparer() != null) {
            return new Expression(visitComparer(ctx.comparer()));
        } else if (ctx.expression().size() == 1) {
            return visitExpression(ctx.expression(0));
        } else {
            Expression left = visitExpression(ctx.expression(0));
            Expression right = visitExpression(ctx.expression(1));
            Expression.ExpressionOp op = null;
            if (ctx.ADD() != null) {
                op = Expression.ExpressionOp.ADD;
            } else if (ctx.SUB() != null) {
                op = Expression.ExpressionOp.SUB;
            } else if (ctx.MUL() != null) {
                op = Expression.ExpressionOp.MUL;
            } else if (ctx.DIV() != null) {
                op = Expression.ExpressionOp.DIV;
            }
            return new Expression(left, right, op);
        }
    }

    /*
    table_constraint :
        K_PRIMARY K_KEY '(' column_name (',' column_name)* ')' ;
     */
    @Override
    public List<String> visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        List<String> columns = new ArrayList<>();

        for (SQLParser.Column_nameContext columnNameContext : ctx.column_name()) {
            columns.add(visitColumn_name(columnNameContext));
        }
        return columns;
    }

    /*
    result_column
        : '*'
        | table_name '.' '*'
        | column_full_name;
     */
    @Override
    public Column visitResult_column(SQLParser.Result_columnContext ctx) {
        if (ctx.getChild(0).getText().equals("*")) {
            return new Column(Global.STAR, null);
        } else if (ctx.getChildCount() > 1) {
            return new Column(Global.STAR, visitTable_name(ctx.table_name()));
        } else {
            return visitColumn_full_name(ctx.column_full_name());
        }
    }

    /*
    table_query :
        table_name
        | table_name K_JOIN table_name + K_ON multiple_condition ;
     */
    @Override
    public TableQuery visitTable_query(SQLParser.Table_queryContext ctx) {
        if (ctx.getChildCount() == 1) {
            return new TableQuery(visitTable_name(ctx.table_name(0)));
        } else {
            return new TableQuery(
                    visitTable_name(ctx.table_name(0)),
                    visitTable_name(ctx.table_name(1)),
                    visitMultiple_condition(ctx.multiple_condition()));
        }
    }

    @Override
    public Object visitAuth_level(SQLParser.Auth_levelContext ctx) {
        return super.visitAuth_level(ctx);
    }

    /*
    literal_value :
        NUMERIC_LITERAL
        | STRING_LITERAL
        | K_NULL ;
     */
    @Override
    public Value visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        if (ctx.NUMERIC_LITERAL() != null) {
            String value = ctx.getText();
            if (value.contains(".") || value.contains("e")) {
                return new Value(ctx.getText(), ValueType.DOUBLE);
            } else {
                return new Value(ctx.getText(), ValueType.INT);
            }
        } else if (ctx.STRING_LITERAL() != null) {
            return new Value(ctx.getText(), ValueType.STRING);
        }
        return new Value(null, ValueType.NULL);
    }

    /*
    column_full_name:
        ( table_name '.' )? column_name ;
    */
    @Override
    public Column visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        String tableName = null;
        if (ctx.table_name() != null) {
            tableName = visitTable_name(ctx.table_name());
        }
        String columnName = visitColumn_name(ctx.column_name());
        return new Column(columnName, tableName);
    }

    /*
    database_name :
        IDENTIFIER ;
    */
    @Override
    public String visitDatabase_name(SQLParser.Database_nameContext ctx) {
        return ctx.IDENTIFIER().getText().toLowerCase();
    }

    /*
    table_name :
        IDENTIFIER ;
     */
    @Override
    public String visitTable_name(SQLParser.Table_nameContext ctx) {
        return ctx.IDENTIFIER().getText().toLowerCase();
    }

    /*
    user_name :
        IDENTIFIER ;
     */
    @Override
    public String visitUser_name(SQLParser.User_nameContext ctx) {
        return ctx.IDENTIFIER().getText();
    }

    /*
    table_name :
        IDENTIFIER ;
     */
    @Override
    public String visitColumn_name(SQLParser.Column_nameContext ctx) {
        return ctx.IDENTIFIER().getText().toLowerCase();
    }

    /*
    view_name :
        IDENTIFIER;
     */
    @Override
    public String visitView_name(SQLParser.View_nameContext ctx) {
        return ctx.IDENTIFIER().getText();
    }

    /*
    password :
        STRING_LITERAL ;
     */
    @Override
    public String visitPassword(SQLParser.PasswordContext ctx) {
        return ctx.STRING_LITERAL().getText();
    }
}
