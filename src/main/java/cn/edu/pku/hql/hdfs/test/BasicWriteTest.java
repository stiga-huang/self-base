package cn.edu.pku.hql.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * classpath里一定要有Hadoop配置文件
 *
 * Created by huangql on 11/11/15.
 */
public class BasicWriteTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                fs.create(new Path("writeTest/0"))));

        writer.write("hello world!\n");
        writer.close();

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("writeTest"), false);
        while (it.hasNext()) {
            LocatedFileStatus f = it.next();
            if (f.isFile()) {
                System.out.println("File: " + f.getPath());
            } else if (f.isDirectory()) {
                System.out.println("Dir: " + f.getPath());
            } else {
                System.out.println("other: " + f.getPath());
            }
        }
        fs.close();
        System.out.println("finished");
    }

}
