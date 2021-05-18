package cn.edu.thssdb.index;

import java.io.*;

public class Testinput {
    public static void main(String[] args) throws IOException {
        //InputStream s = new FileInputStream("123.txt");
        //FileWriter fw = new FileWriter("123.txt");
        //fw.close();
//        FileReader f = new FileReader("123.txt");
//        BufferedReader bf = new BufferedReader(f);
//        try {
//            String k = bf.readLine();
//            System.out.print(k);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        FileWriter fw = new FileWriter("123.txt");
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("974");
//        bw.newLine();
//        bw.write("345");
        bw.newLine();
        bw.close();
        int i = 0;
    }
}
