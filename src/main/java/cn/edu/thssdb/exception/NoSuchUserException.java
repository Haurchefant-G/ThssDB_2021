package cn.edu.thssdb.exception;

public class NoSuchUserException extends RuntimeException {
    String username;

    public NoSuchUserException(String username) {
        this.username = username;
    }

    @Override
    public String getMessage() {
        return "Exception: no user named " + username + "!";
    }
}
