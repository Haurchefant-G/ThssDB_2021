package cn.edu.thssdb.index;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) {
        ByteBuffer rowbuffer = ByteBuffer.allocate(100);
        rowbuffer.putInt(12345);
        rowbuffer.putDouble(1.0);
        rowbuffer.putLong(12345);
        rowbuffer.putFloat(1.0f);
        String l = "abcdef";
        rowbuffer.put(l.getBytes());
        System.out.print(rowbuffer.limit());
        int i = 0;
    }
}
