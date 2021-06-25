package cn.edu.thssdb.exception;

public class PrimaryKeyViolationException extends RuntimeException {

    public PrimaryKeyViolationException() { }

    @Override
    public String getMessage() { return "Exception: Update primary key already exist!"; }
}
