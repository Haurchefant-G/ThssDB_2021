package cn.edu.thssdb.parser;

import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.server.Session;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
            throw e;
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
    public SQLResult createDatabase(CreateDatabaseStatement statement, Session session) {
        try {
            manager.createDatabaseIfNotExists(statement.getDatabaseName());
            return new SQLResult("Database " + statement.getDatabaseName() + " Created.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult useDatabase(UseDatabaseStatement statement, Session session) {
        try {
            manager.switchDatabase(statement.getDatabaseName(), session);
            return new SQLResult("Switch to Database " + statement.getDatabaseName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult showDatabase(ShowDatabasesStatement statement, Session session) {
        return null;
    }

    public SQLResult showTables(ShowTablesStatement statement, Session session) {
        try {
            List<String> heads = Arrays.asList("Table Name");
            List<String> tableNames = manager.getTables(statement.getDatabaseName(), session);
            List<List<String>> stackTableNames = tableNames.stream().map(Arrays::asList).collect(Collectors.toList());

            return new SQLResult("Show tables", heads, stackTableNames, true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult dropDatabase(DropDatabaseStatement statement, Session session) {
        try {
            manager.deleteDatabase(statement.getDatabaseName(), session);
            return new SQLResult("Delete Database " + statement.getDatabaseName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult showTableMeta(ShowTableMetaStatement statement, Session session) {
        try {
            List<String> heads = Arrays.asList("Field", "Data Type", "Not Null", "Primary Key");
            List<List<String>> tableMeta = manager.getTableMeta(statement.getTableName() ,session);
            return new SQLResult("Table " + statement.getTableName() + " meta.", heads, tableMeta, true);

        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    // TABLE
    public SQLResult dropTable(DropTableStatement statement, Session session) {
        try {
            manager.dropTable(statement.getTableName(), session);
            return new SQLResult("Delete Table " + statement.getTableName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult createTable(CreateTableStatement statement, Session session) {
        try {
            manager.createTable(statement.getTableName(), statement.getColumnList(), session);
            return new SQLResult("Create Table " + statement.getTableName(), true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }


    // 用户
    public SQLResult dropUser(DropUserStatement statement, Session session) {
        return null;
    }

    public SQLResult createUser(CreateUserStatement statement, Session session) {
        return null;
    }


    // 增删查改
    public SQLResult insert(InsertStatement statement, Session session) {
        try {
            manager.insert(statement.getTableName(), statement.getColumnNames(), statement.getValues(), session);
            return new SQLResult("Insert succeed", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult select(SelectStatement statement, Session session) {
        try {
            QueryResult queryResult = manager.select(statement.getColumnNames(), statement.getTableQueries(), statement.getWhere(), statement.isDistinct(), session);
            List<String> columnNames = queryResult.getColumnNames();
            List<Row> rows = queryResult.getResults();

            List<List<String>> stringRows = new ArrayList<>();
            for (Row row : rows) {
                List<String> stringRow = row.getEntries().stream().map(Entry::toString).collect(Collectors.toList());
                stringRows.add(stringRow);
            }

            return new SQLResult("Select succeed", columnNames, stringRows,true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(),false);
        }
    }

    public SQLResult delete(DeleteStatement statement, Session session) {
        try {
            int rowNum = manager.delete(statement.getTableName(), statement.getWhere(), session);
            return new SQLResult(rowNum + " row is deleted.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }

    public SQLResult update(UpdateStatement statement, Session session) {
        try {
            int rolNum = manager.update(statement.getTableName(), statement.getColumnName(), statement.getExpression(), statement.getCondition(), session);
            return new SQLResult(rolNum + " row is updated.", true);
        } catch (Exception e) {
            return new SQLResult(e.getMessage(), false);
        }
    }
}
