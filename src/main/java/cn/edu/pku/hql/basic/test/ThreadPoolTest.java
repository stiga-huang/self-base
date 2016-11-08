package cn.edu.pku.hql.basic.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangql on 12/2/15.
 */
public class ThreadPoolTest {

    public static class MyWorker implements Runnable {

        int num;

        public MyWorker(int n) {
            this.num = n;
        }

        @Override
        public void run() {
            num = num / (num - 5);
            for (int i = 0; i < 100; i++)
                System.out.println(System.currentTimeMillis() + "\t" + Thread.currentThread().getName() + ": " + num);
        }
    }

    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 10; i++)
            pool.execute(new Thread(new MyWorker(i)));

    }
}
