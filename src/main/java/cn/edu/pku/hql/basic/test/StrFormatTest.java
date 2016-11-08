package cn.edu.pku.hql.basic.test;

import java.util.Arrays;

/**
 * Test for String.format
 * commands: java com.mininglamp.hbase.test.StrFormatTest '%1$s haha %2$s' abc 100
 *
 * Created by huangql on 8/30/15.
 */
public class StrFormatTest {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("args.length: " + args.length + ", only accept 3 args");
            return;
        }
        //String strFormat = "%1$s haha %2$s";
        String strFormat = args[0];
        String[] vars = Arrays.copyOfRange(args, 1, args.length);
        String res = String.format(strFormat, vars);
        System.out.println(res);

        System.out.println("------- test 2 --------");
        String strFormat2 = "%010d 到哪";
        res = String.format(strFormat2, 144098);
        System.out.println(res);
    }
}
