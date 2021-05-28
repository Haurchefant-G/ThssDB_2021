package cn.edu.thssdb.exception;

public class UserExistsException extends RuntimeException {
    String username;

    public UserExistsException(String username) {
        this.username = username;
    }

    @Override
    public String getMessage() {
        return "Exception: " + username + " exists.";
    }

}
