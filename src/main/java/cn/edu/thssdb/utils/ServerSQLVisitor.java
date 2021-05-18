package cn.edu.thssdb.utils;

import cn.edu.thssdb.parser.SQLBaseVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;


import java.util.*;

public class ServerSQLVisitor extends SQLBaseVisitor<Object> {

    private static Manager manager = Manager.getInstance();

    private QueryTable queryTable;

    private ExecuteStatementResp resp;

    public ServerSQLVisitor() {

    }

    public ServerSQLVisitor(String Dbname) {
        manager.switchDatabase(Dbname);
    }

    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {

        ctx.sql_stmt_list().accept(this);
        return super.visitParse(ctx);
    }

    @Override
    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        //ctx.sql_stmt();
        resp = null;
        resp = new ExecuteStatementResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        for (Iterator<SQLParser.Sql_stmtContext> i = ctx.sql_stmt().iterator(); i.hasNext(); ) {
            SQLParser.Sql_stmtContext sql = i.next();
            sql.children.get(0).accept(this);
        }
        return resp;
    }

    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        ArrayList<Column> columns = new ArrayList<>();

        ArrayList<String> names = new ArrayList();
        ArrayList<ColumnType> types = new ArrayList();
        ArrayList<Integer> maxLens = new ArrayList();
        ArrayList<Integer> primarys = new ArrayList();
        ArrayList<Boolean> notNulls = new ArrayList();
        for (Iterator<SQLParser.Column_defContext> i = ctx.column_def().iterator(); i.hasNext(); ) {
            SQLParser.Column_defContext colunmDef = i.next();
            //ColumnType type = ColumnType.valueOf(colunmDef.type_name().getText().toUpperCase());
            String type = colunmDef.type_name().getText().toUpperCase();
            Integer maxLength = 4, primary = 0;
            boolean notNull = false;
            switch (type) {
                case "INT":
                case "FLOAT":
                    maxLength = 4;
                    break;
                case "LONG":
                case "DOUBLE":
                    maxLength = 8;
                    break;
                default:
                    if (colunmDef.type_name().T_STRING() != null) {
                        maxLength = Integer.parseInt(colunmDef.type_name().NUMERIC_LITERAL().getText());
                    }
                    type = "STRING";
                    break;
            }
            for (Iterator<SQLParser.Column_constraintContext> j = colunmDef.column_constraint().iterator(); j.hasNext(); ) {
                SQLParser.Column_constraintContext constraint = j.next();
                if (constraint.K_PRIMARY() != null) {
                    primary = 1;
                    notNull = true;
                }
                if (constraint.K_NOT() != null) {
                    notNull = true;
                }
            }
            names.add(colunmDef.column_name().getText());
            types.add(ColumnType.valueOf(type));
            primarys.add(primary);
            notNulls.add(notNull);
            maxLens.add(maxLength);
        }
        for (Iterator<SQLParser.Column_nameContext> i = ctx.table_constraint().column_name().iterator(); i.hasNext(); ) {
            String name = i.next().getText();
            int index = names.indexOf(name);
            primarys.set(index, 1);
        }
        while (!names.isEmpty()) {
            columns.add(new Column(names.remove(0), types.remove(0), primarys.remove(0), notNulls.remove(0), maxLens.remove(0)));
        }

        manager.selectDatabase.create(tableName, columns);
        return null;
    }

    @Override
    public Object visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {

        return null;

    }

    @Override
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        manager.selectDatabase.drop(ctx.table_name().getText());
        return null;
    }

    @Override
    public Object visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        return super.visitDrop_db_stmt(ctx);
    }

    private Entry valueEntry(ColumnType type, String value) {
        Entry entry;
        switch (type) {
            case INT:
                entry = new Entry(Integer.parseInt(value));
                break;
            case FLOAT:
                entry = new Entry(Float.parseFloat(value));
                break;
            case DOUBLE:
                entry = new Entry(Double.parseDouble(value));
                break;
            case LONG:
                entry = new Entry(Long.parseLong(value));
                break;
            case STRING:
                entry = new Entry(value.substring(1, value.length() - 1));
                break;
            default:
                entry = new Entry(null);
        }
        return entry;
    }

    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        // TODO
        List<Row> insertRows = new ArrayList<>();
        String tableName = ctx.table_name().getText();
        ArrayList<Column> columns = manager.selectDatabase.getColumns(tableName);
        if (columns == null) {
            resp.status.setCode(Global.FAILURE_CODE);
            resp.status.setMsg("当前数据库不存在该表");
            return null;
        }
        Entry[] entries = new Entry[columns.size()];
        List<SQLParser.Value_entryContext> value_entries = ctx.value_entry();
        if (ctx.column_name().size() == 0) {
            for (SQLParser.Value_entryContext value_entry:
                 value_entries) {
                List<SQLParser.Literal_valueContext> values = value_entry.literal_value();
                if (values.size() == columns.size()) {
                    for (int i = 0; i < columns.size(); i++) {
                        entries[i] = valueEntry(columns.get(i).getType(), values.get(i).getText());
                    }
                    Row row = new Row(entries);
                    insertRows.add(row);
                    //manager.selectDatabase.insertRow(tableName, row);
                } else {
                    //给出的值的个数不够
                    resp.status.setCode(Global.FAILURE_CODE);
                    resp.status.setMsg("给的值个数不够");
                    return null;
                }
            }
        } else {
            ArrayList<String> insertColumns = new ArrayList<>();
            for (SQLParser.Column_nameContext c : ctx.column_name()) {
                String colname = c.getText();
                if (columns.contains(colname)) {
                    insertColumns.add(c.getText());
                } else {
                    //有没有的字段
                    resp.status.setCode(Global.FAILURE_CODE);
                    resp.status.setMsg(tableName + "不包含" + colname + "字段");
                    return null;
                }
            }
            for (SQLParser.Value_entryContext value_entry:
                    value_entries) {
                List<SQLParser.Literal_valueContext> values = value_entry.literal_value();
                if (values.size() == insertColumns.size()) {
                    for (int i = 0; i < columns.size(); i++) {
                        boolean hasValue = false;
                        for (int j = 0; j < insertColumns.size(); j++) {
                            if (columns.get(i).getName().equals(insertColumns.get(j))) {
                                entries[i] = valueEntry(columns.get(i).getType(), values.get(j).getText());
                                hasValue = true;
                                //insertColumns.remove(j); //移除已经赋值的字段
                                break;
                            }
                        }
                        if (!hasValue) {
                            if (columns.get(i).NotNull()) {
                                //throw Exception; 有字段不能为空
                                resp.status.setCode(Global.FAILURE_CODE);
                                resp.status.setMsg(columns.get(i).getName() + "字段不能为空");
                                return null;
                            } else {
                                entries[i] = new Entry(null);
                            }
                        }
                    }
                    Row row = new Row(entries);
                    insertRows.add(row);
                } else {
                    //赋值不够
                    resp.status.setCode(Global.FAILURE_CODE);
                    resp.status.setMsg("给的值个数不够");
                    return null;
                }
            }
        }
        for (Row row: insertRows) {
            manager.selectDatabase.insertRow(tableName, row);
        }
        manager.selectDatabase.commitTable(tableName);
        //manager.selectDatabase.insertRow(tableName, row);
        //return super.visitInsert_stmt(ctx);
        return null;
    }

    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        ArrayList<Row> result = null;
        if (ctx.AND() != null) {
            System.out.print(ctx.multiple_condition(0).getText());
            ctx.multiple_condition(0).accept(this);
            System.out.print(ctx.multiple_condition(1).getText());
            ctx.multiple_condition(1).accept(this);
        } else if (ctx.OR() != null) {
            System.out.print(ctx.multiple_condition(0).getText());
            ctx.multiple_condition(0).accept(this);
            System.out.print(ctx.multiple_condition(1).getText());
            ctx.multiple_condition(1).accept(this);
        } else {
            int attrIndex = queryTable.getColumnIndex(ctx.condition().expression(0).getText());
            if (attrIndex != -1) {
                String comparator = ctx.condition().comparator().getText();
                String attrValue = ctx.condition().expression(0).getText();
                result = queryTable.queryRow(attrIndex, comparator, attrValue);
                return result;
            }

        }
        return result;
    }

    @Override
    public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        if (ctx.K_WHERE() == null) {
            //删除所有
        } else {
            queryTable = manager.selectDatabase.getTabelQuery(ctx.table_name().getText());
            ArrayList<Row> result = (ArrayList<Row>) ctx.multiple_condition().accept(this);
            if (result != null) {
                manager.selectDatabase.deleteRows(queryTable.getTableName(), result);
            }
        }
        //System.out.print(i);
        return super.visitDelete_stmt(ctx);
    }

    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        int updateindex = queryTable.getColumnIndex(ctx.column_name().getText());
        if (updateindex >= 0) {
            String tablename = ctx.table_name().getText();
            queryTable = manager.selectDatabase.getTabelQuery(ctx.table_name().getText());
            ArrayList<Row> rows = (ArrayList<Row>) ctx.multiple_condition().accept(this);
            if (rows != null) {
                ColumnType type = queryTable.getColunmnType(updateindex);
                ArrayList<Row> newrows = new ArrayList<>();
                for (Row r : rows) {
                    Entry updateEntry = valueEntry(type, ctx.expression().getText());
                    Row newr = r.updateRow(updateindex, updateEntry);
                    newrows.add(newr);
                }
                manager.selectDatabase.updateRows(tablename, rows, newrows);
            }
        }
        return super.visitUpdate_stmt(ctx);
    }

    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        List<SQLParser.Table_queryContext> tableQuueries = ctx.table_query();
        ArrayList<String> tableNames = new ArrayList<>();

        String where = ctx.multiple_condition().getText();
        Expression whereExp = AviatorEvaluator.compile("");
        Map<String, Object> whereEnv = new HashMap<String, Object>();
        List<String> whereArgs = whereExp.getVariableNames();

        resp.columnsList = new ArrayList<>();
        resp.rowList = new ArrayList<>();

        if (where != null) {
            whereExp = AviatorEvaluator.compile(where);
            whereArgs = whereExp.getVariableNames();
        }
        if (tableQuueries.size() == 1) {
            for (SQLParser.Table_nameContext name: tableQuueries.get(0).table_name()) {
                tableNames.add(name.getText());
            }
            if (tableNames.size() > 1) {
                boolean []selectAll = new boolean[2];
                for (int k = 0; k < 2; ++k) {
                    selectAll[k] = false;
                }
                Row []rows = new Row[2];
                String on = tableQuueries.get(0).multiple_condition().getText();
                Expression onExp = AviatorEvaluator.compile(on);
                List<String> onArgs = onExp.getVariableNames();
                Map<String, Object> onEnv = new HashMap<String, Object>();
                int []argRowIndex = new int[onArgs.size()];
                int []argColumnIndex = new int[onArgs.size()];
                MetaInfo []infos = new MetaInfo[2];
                ArrayList<Column> table0column = manager.selectDatabase.getColumns(tableNames.get(0));
                infos[0] = new MetaInfo(tableNames.get(0), table0column);
                ArrayList<Column> table1column = manager.selectDatabase.getColumns(tableNames.get(1));
                infos[1] = new MetaInfo(tableNames.get(1), table0column);
                int j = 0;
                for (String arg: onArgs) {
                    int index = infos[0].columnFind(arg);
                    if (index >=0) {
                        argRowIndex[j] = 0;
                        argColumnIndex[j] = index;
                    } else {
                        index = infos[1].columnFind(arg);
                        if (index >= 0 ) {
                            argRowIndex[j] = 1;
                            argColumnIndex[j] = index;
                        } else {
                            //没有找到该属性，sql命令有错误
                        }
                    }
                    ++j;
                }
                argRowIndex = new int[whereArgs.size()];
                argColumnIndex = new int[whereArgs.size()];
                j = 0;
                for (String arg: whereArgs) {
                    int index = infos[0].columnFind(arg);
                    if (index >=0) {
                        argRowIndex[j] = 0;
                        argColumnIndex[j] = index;
                    } else {
                        index = infos[1].columnFind(arg);
                        if (index >= 0 ) {
                            argRowIndex[j] = 1;
                            argColumnIndex[j] = index;
                        } else {
                            //没有找到该属性，sql命令有错误
                        }
                    }
                    ++j;
                }

                ArrayList<Integer> attrRowIndex = new ArrayList<>();
                ArrayList<Integer> attrColumnIndex = new ArrayList<>();
                for (SQLParser.Result_columnContext c : ctx.result_column()) {
                    SQLParser.Column_full_nameContext column_full_name = c.column_full_name();
                    if (column_full_name != null) {
                        SQLParser.Table_nameContext table_name = column_full_name.table_name();
                        if (table_name != null) {
                            int rowIndex = tableNames.indexOf(table_name.getText());
                            if (rowIndex >= 0) {
                                //attrRowIndex.add(rowIndex);
                                String column_name = column_full_name.column_name().getText();
                                int colIndex = infos[rowIndex].columnFind(column_name);
                                if (colIndex >= 0) {
                                    attrRowIndex.add(rowIndex);
                                    attrColumnIndex.add(colIndex);
                                } else {
                                    //选择了不存在的属性，报错
                                }
                            } else {
                                //选择了不存在的属性，报错
                            }
                        } else {
                            String column_name = column_full_name.column_name().getText();
                            int colIndex = infos[0].columnFind(column_name);
                            if (colIndex >=0) {
                                attrRowIndex.add(0);
                                attrColumnIndex.add(colIndex);
                            } else {
                                colIndex = infos[1].columnFind(column_name);
                                if (colIndex >= 0 ) {
                                    attrRowIndex.add(1);
                                    attrColumnIndex.add(colIndex);
                                } else {
                                    //没有找到该属性，sql命令有错误
                                }
                            }
                        }
                        continue;
                    }
                    SQLParser.Table_nameContext table_name = column_full_name.table_name();
                    if (table_name != null) {
                        int rowIndex = tableNames.indexOf(table_name.getText());
                        if (rowIndex >= 0) {
                            selectAll[rowIndex] = true;
                        } else {
                            //选择了不存在的表，报错
                        }
                        continue;
                    }
                    String attr = c.getText();
                    if (attr.equals("*")) {
                        for (int k = 0; k < 2; ++k) {
                            selectAll[k] = true;
                        }
                    }
                }
                for (int l = 0; l < 2; ++l) {
                    if (selectAll[l]) {
                        infos[l].addToStringList(resp.columnsList);
                    }
                }
                Iterator<Integer> attrRowIt = attrRowIndex.iterator();
                Iterator<Integer> attrColIt = attrColumnIndex.iterator();
                for (; attrRowIt.hasNext();) {
                    resp.columnsList.add(infos[attrRowIt.next()].columnName(attrColIt.next(), true));
                }
                for (QueryTable p = manager.selectDatabase.getTabelQuery(tableNames.get(0)); p.hasNext();) {
                    rows[0] = p.next();
                    for (QueryTable q = manager.selectDatabase.getTabelQuery(tableNames.get(1)); q.hasNext();) {
                        rows[1] = q.next();
                        int k = 0;
                        for (String arg: onArgs) {
                            onEnv.put(arg, rows[argRowIndex[k]].getEntry(argColumnIndex[k]).value);
                        }
                        Boolean onResult = (Boolean) onExp.execute(onEnv);
                        if (onResult) {
                            if (where != null) {
                                for (String arg: whereArgs) {
                                    whereEnv.put(arg, rows[argRowIndex[k]].getEntry(argColumnIndex[k]).value);
                                }
                                if((Boolean) whereExp.execute(whereEnv)) {
                                    List<String> aRow = new ArrayList<>();
                                    for (int l = 0; l < 2; ++l) {
                                        if (selectAll[l]) {
                                            rows[l].addToStringList(aRow);
                                        }
                                    }
                                    attrRowIt = attrRowIndex.iterator();
                                    attrColIt = attrColumnIndex.iterator();
                                    for (; attrRowIt.hasNext();) {
                                        aRow.add(rows[attrRowIt.next()].getEntry(attrColIt.next()).toString());
                                    }
                                    resp.rowList.add(aRow);
                                };
                            }
                        }
                    }
                }
            }
        }
        return super.visitSelect_stmt(ctx);
    }
}
