package cn.edu.thssdb.parser;

import java.util.List;

public class SQLResult {
    String msg;
    List<String> columnList;
    List<List<String>> rowList;
    boolean succeed;
    boolean isAbort;
    boolean hasResult;

    public SQLResult(String msg, boolean succeed) {
        this.msg = msg;
        this.succeed = succeed;
    }

    public SQLResult(String msg, List<String> columnList, List<List<String>> rowList, boolean succeed) {
        this.msg = msg;
        this.columnList = columnList;
        this.rowList = rowList;
        this.succeed = succeed;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    public List<List<String>> getRowList() {
        return rowList;
    }

    public void setRowList(List<List<String>> rowList) {
        this.rowList = rowList;
    }

    public boolean isSucceed() {
        return succeed;
    }

    public void setSucceed(boolean succeed) {
        this.succeed = succeed;
    }

    public boolean isAbort() {
        return isAbort;
    }

    public void setAbort(boolean abort) {
        isAbort = abort;
    }

    public boolean isHasResult() {
        return hasResult;
    }

    public void setHasResult(boolean hasResult) {
        this.hasResult = hasResult;
    }
}
