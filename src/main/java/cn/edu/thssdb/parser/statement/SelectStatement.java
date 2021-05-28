package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.query.TableQuery;
import cn.edu.thssdb.query.Where;
import cn.edu.thssdb.schema.Column;

import java.util.List;

public class SelectStatement extends Statement {
    List<String> columnNames;
    List<TableQuery> tableQueries;
    Where where;
    boolean distinct;

    public SelectStatement(List<String> columnNames, List<TableQuery> tableQueries, Where where, boolean distinct) {
        this.columnNames = columnNames;
        this.tableQueries = tableQueries;
        this.where = where;
        this.distinct = distinct;
    }

    public StatementType getType() { return StatementType.SELECT; }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<TableQuery> getTableQueries() {
        return tableQueries;
    }

    public void setTableQueries(List<TableQuery> tableQueries) {
        this.tableQueries = tableQueries;
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }
}
