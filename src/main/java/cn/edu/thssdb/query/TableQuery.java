package cn.edu.thssdb.query;

public class TableQuery {
    private String tableLeft = null;
    private String tableRight = null;
    private Where where = null;

    public TableQuery(String tableLeft) {
        this.tableLeft = tableLeft;
    }

    public TableQuery(String tableLeft, String tableRight, Where where) {
        this.tableLeft = tableLeft;
        this.tableRight = tableRight;
        this.where = where;
    }

    public String getTableLeft() {
        return tableLeft;
    }

    public String getTableRight() {
        return tableRight;
    }

    public Where getWhere() {
        return where;
    }


}
