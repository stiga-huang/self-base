package cn.edu.pku.hql.basic.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for "assert"
 *
 * Created by huangql on 8/30/15.
 */
public class AssertTest {

    public static void main(String[] args) {
        assert args.length == 3;
        System.out.println("finished");

        List<String> lines = new ArrayList<String>(16);
        System.out.println("size: " + lines.size());
        lines.add("abc");
        System.out.println("size: " + lines.size());

        String line = "a\te\t\tee\t\t";
        String[] ss = line.split("\t", 100);
        //if (ss[4] == null)
        //    System.out.println("null");
        System.out.println(ss[2]);
        System.out.println(Arrays.toString(ss));
    }
}
