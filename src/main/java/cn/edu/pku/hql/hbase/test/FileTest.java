package cn.edu.pku.hql.hbase.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Test for file
 * Created by huangql on 9/9/15.
 */
public class FileTest {

    public static void main(String[] args) throws IOException {
        File f = new File("d/abc"); // 只要目录存在，可直接在其内创建文件
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write("hello!");
        bw.close();
    }
}
