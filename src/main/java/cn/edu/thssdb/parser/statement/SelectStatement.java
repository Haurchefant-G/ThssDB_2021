package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.TableQuery;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.schema.Column;

import java.util.List;

public class SelectStatement extends Statement {
    List<Column> columns;
    List<TableQuery> tableQueries;
    Where condition;
    boolean distinct;

    public SelectStatement(List<Column> columns, List<TableQuery> tableQueries, Where condition, boolean distinct) {
        this.columns = columns;
        this.tableQueries = tableQueries;
        this.condition = condition;
        this.distinct = distinct;
    }

    public StatementType getType() { return StatementType.SELECT; }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<TableQuery> getTableQueries() {
        return tableQueries;
    }

    public void setTableQueries(List<TableQuery> tableQueries) {
        this.tableQueries = tableQueries;
    }

    public Where getCondition() {
        return condition;
    }

    public void setCondition(Where condition) {
        this.condition = condition;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }
}
