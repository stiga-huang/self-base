package cn.edu.pku.hql.hbase.test;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by huangql on 9/7/15.
 */
public class BytesTest {
    public static void main(String[] args) {
        String str = "abcde";
        byte[] a = str.getBytes();
        a[a.length - 1] += 1;
        System.out.println(str);
        System.out.println(new String(a));

        str = "";
        a = Bytes.toBytes(str);
        System.out.println(new String(a));
        a = str.getBytes();
        System.out.println(new String(a));
    }
}
