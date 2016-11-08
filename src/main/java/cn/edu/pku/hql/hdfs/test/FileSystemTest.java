package cn.edu.pku.hql.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by huangql on 4/9/16.
 */
public class FileSystemTest {
    public static void main(String[] args) throws IOException {
        FileSystem fs1 = FileSystem.get(new Configuration());
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs1.open(new Path("NLineInputFormatTest/a.txt"))));

        FileSystem fs2 = FileSystem.get(new Configuration());
        fs2.close();

        if (fs1 == fs2)
            System.out.println("same FileSystem object!");

        reader.readLine();
    }
}
