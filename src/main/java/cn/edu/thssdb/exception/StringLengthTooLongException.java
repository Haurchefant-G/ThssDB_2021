package cn.edu.thssdb.exception;

public class StringLengthTooLongException extends RuntimeException {
    int length;
    int constraint;

    public StringLengthTooLongException(int length, int constraint) {
        this.length = length;
        this.constraint = constraint;
    }

    @Override
    public String getMessage() { return "Exception: String length too long. Given " + length + " but the maximum length allowed is " + constraint; }
}
