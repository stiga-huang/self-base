package cn.edu.pku.hql.basic.test;

import java.util.Arrays;

/**
 * Created by huangql on 9/2/15.
 */
public class SplitTest {

    public static void main(String[] args) {
        String str = "abc";
        String[] ss = str.split(",");
        System.out.println(ss.length);
        System.out.println(Arrays.toString(ss));

        str = "a,b,c,d";
        ss = str.split(",", 2);
        System.out.println(ss.length);
        System.out.println(Arrays.toString(ss));

        ss = str.split(",");
        System.out.println(ss.length);
        System.out.println(Arrays.toString(ss));
    }
}
