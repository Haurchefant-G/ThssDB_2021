package cn.edu.thssdb.utils;

public class debug {
    public static void main(String[] args) {
        Comparable a = 1001890910;

        System.out.println(a instanceof Integer);

        Number value = ((Number)a);

        System.out.println(value.intValue() == value.doubleValue());

//        System.out.println(value.toString());

//        System.out.println(value + 2);
    }
}
