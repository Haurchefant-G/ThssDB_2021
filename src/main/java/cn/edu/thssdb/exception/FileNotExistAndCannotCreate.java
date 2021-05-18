package cn.edu.thssdb.exception;

public class FileNotExistAndCannotCreate extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: file doesn't exist! and can't be created";
    }
}
