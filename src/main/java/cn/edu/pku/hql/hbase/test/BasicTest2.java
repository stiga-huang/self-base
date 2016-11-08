package cn.edu.pku.hql.hbase.test;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Test for java.util.ConcurrentModificationException
 *
 * Created by huangql on 8/28/15.
 */
public class BasicTest2 {

    public static void main(String[] args) {
        ArrayList<String> li = new ArrayList<String>();

        for (int i = 0; i < 10; i++)
            li.add(""+i);

        try {
            Iterator<String> it = li.iterator();
            while (it.hasNext()) {
                System.out.println(it.next());
                li.remove(2);
            }
        } catch (Exception e) {
            System.err.println("catch exception");
            e.printStackTrace();
        }
    }
}
