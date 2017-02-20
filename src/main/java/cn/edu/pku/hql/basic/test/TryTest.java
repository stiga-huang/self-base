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

    /**
     * try中的变量初始化语句抛出的异常也能被catch捕获, 对象的close函数不会被调用
     */
    public static void test2() {
        try (MyCloseable c = new MyCloseable(true)) {
            c.Do();
        } catch (Exception e) {
            System.out.println("catch exception in var generation");
        }
    }

    public static void main(String[] args) {
        test1();
        test2();
    }
}
class MyException extends Exception {

}
class MyCloseable implements Closeable {

    public MyCloseable() {
        System.out.println("call <init>");
    }

    public MyCloseable(Boolean throwException) throws MyException {
        if (throwException)
            throw new MyException();
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
