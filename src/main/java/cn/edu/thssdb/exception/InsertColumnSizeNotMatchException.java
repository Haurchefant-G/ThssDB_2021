package cn.edu.thssdb.exception;

public class InsertColumnSizeNotMatchException extends RuntimeException {
    long colNum;
    long valueNum;

    public InsertColumnSizeNotMatchException(long colNum, long valueNum) {
        this.colNum = colNum;
        this.valueNum = valueNum;
    }

    @Override
    public String getMessage() { return "Exception: insert column number not match! given " + valueNum + " value while " + colNum + " is needed."; }
}
