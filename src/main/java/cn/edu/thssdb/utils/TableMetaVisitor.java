package cn.edu.thssdb.utils;

import cn.edu.thssdb.parser.SQLBaseVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.schema.Column;

import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 从.db元数据的一行创建table
 */
public class TableMetaVisitor extends SQLBaseVisitor<MetaInfo> {

    /**
     * 解析.db元数据一行创建对应table
     *
     * @param ctx
     * @return
     */
    @Override
    public MetaInfo visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
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
            //columns.add(new Column(name, type, primary, notNull, maxLength));
        }
        for (Iterator<SQLParser.Column_nameContext> i = ctx.table_constraint().column_name().iterator(); i.hasNext(); ) {
            String name = i.next().getText();
            int index = names.indexOf(name);
            primarys.set(index, 1);
            notNulls.set(index, true);
        }
        while (!names.isEmpty()) {
            columns.add(new Column(names.remove(0), types.remove(0), primarys.remove(0), notNulls.remove(0), maxLens.remove(0)));
        }
        return new MetaInfo(tableName, columns);
    }

}
