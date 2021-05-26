package cn.edu.thssdb.parser;

import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.server.Session;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SQLProcessor {
    Manager manager;

    public SQLProcessor(Manager manager) {
        this.manager = manager;
    }

    public List<Statement> parseSQL(String sql) {
        try {
            CharStream stream = CharStreams.fromString(sql);
            SQLLexer lexer = new SQLLexer(stream);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SQLParser parser = new SQLParser(tokens);
            ParseTree tree = parser.parse();
            MySQLVisitor visitor = new MySQLVisitor();
            return (List<Statement>) visitor.visit(tree);
        }
        catch (Exception e) {
            return null;
        }
    }

    public List<SQLResult> executeSQL(List<Statement> statementList, Session session) {
        ArrayList<SQLResult> results = new ArrayList<>();
        for (Statement statement : statementList) {
            if (statement.getType() == StatementType.CREATE_DATABASE) {
                results.add(createDatabase((CreateDatabaseStatement) statement, session));
            } else if (statement.getType() == StatementType.DROP_DATABASE) {
                results.add(dropDatabase((DropDatabaseStatement) statement, session));
            } else if (statement.getType() == StatementType.SHOW_DATABASE) {
                results.add(showDatabase((ShowDatabasesStatement) statement, session));
            } else if (statement.getType() == StatementType.USE_DATABASE) {
                results.add(useDatabase((UseDatabaseStatement) statement, session));
            } else if (statement.getType() == StatementType.CREATE_TABLE) {
                results.add(createTable((CreateTableStatement) statement, session));
            } else if (statement.getType() == StatementType.SHOW_TABLES) {
                results.add(showTables((ShowTablesStatement) statement, session));
            } else if (statement.getType() == StatementType.DROP_TABLE) {
                results.add(dropTable((DropTableStatement) statement, session));
            } else if (statement.getType() == StatementType.SHOW_TABLE_META) {
                results.add(showTableMeta((ShowTableMetaStatement) statement, session));
            } else if (statement.getType() == StatementType.CREATE_USER) {
                results.add(createUser((CreateUserStatement) statement, session));
            } else if (statement.getType() == StatementType.DROP_USER) {
                results.add(dropUser((DropUserStatement) statement, session));
            } else if (statement.getType() == StatementType.INSERT) {
                results.add(insert((InsertStatement) statement, session));
            } else if (statement.getType() == StatementType.SELECT) {
                results.add(select((SelectStatement) statement, session));
            } else if (statement.getType() == StatementType.UPDATE) {
                results.add(update((UpdateStatement) statement, session));
            } else if (statement.getType() == StatementType.DELETE) {
                results.add(delete((DeleteStatement) statement, session));
            } else {

            }
        }
        return results;
    }

    // DATABASE
    private SQLResult createDatabase(CreateDatabaseStatement statement, Session session) {
        try {
            manager.createDatabaseIfNotExists(statement.getDatabaseName());
            return new SQLResult("Database " + statement.getDatabaseName() + " Created.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult useDatabase(UseDatabaseStatement statement, Session session) {
        try {
            manager.switchDatabase(statement.getDatabaseName(), session);
            return new SQLResult("Switch to Database " + statement.getDatabaseName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult showDatabase(ShowDatabasesStatement statement, Session session) {
        return null;
    }

    private SQLResult dropDatabase(DropDatabaseStatement statement, Session session) {
        try {
            manager.deleteDatabase(statement.getDatabaseName(), session);
            return new SQLResult("Delete Database " + statement.getDatabaseName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult showTableMeta(ShowTableMetaStatement statement, Session session) {
        try {
            return manager.getTableMeta(statement.getTableName() ,session);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    // TABLE
    private SQLResult dropTable(DropTableStatement statement, Session session) {
        try {
            manager.dropTable(statement.getTableName(), session);
            return new SQLResult("Delete Table " + statement.getTableName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult showTables(ShowTablesStatement statement, Session session) {
        return null;
    }

    private SQLResult createTable(CreateTableStatement statement, Session session) {
        try {
            manager.createTable(statement.getTableName(), statement.getColumnList(), session);
            return new SQLResult("Create Table " + statement.getTableName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }


    // 用户
    private SQLResult dropUser(DropUserStatement statement, Session session) {
        return null;
    }

    private SQLResult createUser(CreateUserStatement statement, Session session) {
        return null;
    }


    // 增删查改
    private SQLResult insert(InsertStatement statement, Session session) {
        try {
            manager.insert(statement.getTableName(), statement.getColumnNames(), statement.getValues(), session);
            return new SQLResult("Insert succeed", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult select(SelectStatement statement, Session session) {
        try {
            return manager.select(statement.getColumns(), statement.getTableQueries(), statement.getCondition(), statement.isDistinct(), session);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult delete(DeleteStatement statement, Session session) {
        try {
            int rowNum = manager.delete(statement.getTableName(), statement.getCondition(), session);
            return new SQLResult(rowNum + " row is deleted.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    private SQLResult update(UpdateStatement statement, Session session) {
        try {
            int rolNum = manager.update(statement.getTableName(), statement.getColumnName(), statement.getExpression(), statement.getCondition(), session);
            return new SQLResult(rolNum + " row is updated.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }
}
