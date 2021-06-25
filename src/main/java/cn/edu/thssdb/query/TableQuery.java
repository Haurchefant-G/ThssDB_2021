package cn.edu.thssdb.query;

public class TableQuery {
    private String tableLeft = null;
    private String tableRight = null;
    private Where where = null;
    private boolean outerLeft = false;
    private boolean outerRight = false;

    public TableQuery(String tableLeft) {
        this.tableLeft = tableLeft;
    }

    public TableQuery(String tableLeft, String tableRight, Where where, boolean outerLeft, boolean outerRight) {
        this.tableLeft = tableLeft;
        this.tableRight = tableRight;
        this.where = where;
        this.outerLeft = outerLeft;
        this.outerRight = outerRight;
    }

    public String getTableLeft() {
        return tableLeft;
    }

    public void setTableLeft(String tableLeft) {
        this.tableLeft = tableLeft;
    }

    public String getTableRight() {
        return tableRight;
    }

    public void setTableRight(String tableRight) {
        this.tableRight = tableRight;
    }

    public Where getWhere() {
        return where;
    }

    public void setWhere(Where where) {
        this.where = where;
    }

    public boolean isOuterLeft() {
        return outerLeft;
    }

    public void setOuterLeft(boolean outerLeft) {
        this.outerLeft = outerLeft;
    }

    public boolean isOuterRight() {
        return outerRight;
    }

    public void setOuterRight(boolean outerRight) {
        this.outerRight = outerRight;
    }
}
