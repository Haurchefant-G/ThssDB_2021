package cn.edu.thssdb.utils;

import cn.edu.thssdb.parser.SQLBaseVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.Database;

/**
 * 从.meta元数据的一行创建database
 */
public class DatabaseMetaVisitor extends SQLBaseVisitor<Database> {

    /**
     * 解析.meta元数据一行创建对应database
     * @param ctx
     * @return
     */
    @Override
    public Database visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String name = ctx.database_name().getText();
        return new Database(name);
    }
}
