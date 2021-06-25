package cn.edu.thssdb.utils;

import java.io.*;

public class ObjectRandomAccessFIle extends RandomAccessFile implements ObjectInput, ObjectOutput {

    public ObjectRandomAccessFIle(String name, String mode) throws FileNotFoundException {
        super(name, mode);
    }

    @Override
    public Object readObject() throws ClassNotFoundException, IOException {
        return null;
    }

    @Override
    public long skip(long n) throws IOException {
        for(; n > Integer.MAX_VALUE; n -= Integer.MAX_VALUE) {
            skipBytes(Integer.MAX_VALUE);
        }
        skipBytes((int) n);
        return n;
    }

    @Override
    public int available() throws IOException {
        long available = length() - getFilePointer();
        return (available > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) available;
    }

    @Override
    public void writeObject(Object obj) throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }
}
