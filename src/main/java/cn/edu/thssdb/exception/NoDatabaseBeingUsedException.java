package cn.edu.thssdb.exception;

public class NoDatabaseBeingUsedException extends RuntimeException{
    public NoDatabaseBeingUsedException() {}

    @Override
    public String getMessage() { return "Exception: no database is being used"; }
}
