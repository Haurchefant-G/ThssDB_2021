package cn.edu.thssdb.exception;

public class WrongPasswordException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: wrong password!";
    }
}
