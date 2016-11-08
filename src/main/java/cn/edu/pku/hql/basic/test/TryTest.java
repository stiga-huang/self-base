package cn.edu.pku.hql.basic.test;

import java.io.Closeable;
import java.io.IOException;

/**
 * Test for try-with-resources
 * Created by huangql on 9/14/15.
 */
public class TryTest {

    /**
     * 不管语句中是否有中途跳出，try括号内的对象一定会执行close()，且在finally执行之前。
     */
    public static void test1() {
        try (MyCloseable c = new MyCloseable()) {
            if (0 == 0) return;
            c.Do();
            return;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("finally");
        }
    }

    public static void main(String[] args) {
        test1();
    }
}
class MyCloseable implements Closeable {

    public MyCloseable() {
        System.out.println("call <init>");
    }

    public void Do() throws InterruptedException {
        System.out.println("sleep");
        Thread.sleep(1000);
    }

    @Override
    public void close() throws IOException {
        System.out.println("calling close()");
    }
}
