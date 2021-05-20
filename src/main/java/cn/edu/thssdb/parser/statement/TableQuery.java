package cn.edu.thssdb.parser.statement;

public class TableQuery {
    private String tableLeft;
    private String tableRight;
    private Where where;

    public TableQuery(String tableLeft) {
        this.tableLeft = tableLeft;
    }

    public TableQuery(String tableLeft, String tableRight, Where where) {
        this.tableLeft = tableLeft;
        this.tableRight = tableRight;
        this.where = where;
    }
}
