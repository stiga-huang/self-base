package cn.edu.pku.hql.basic.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by quanlong.huang on 4/7/17.
 */
public class FinalVarTest {
  public static void main(String[] args) throws InterruptedException {
    ExecutorService exec = Executors.newFixedThreadPool(10);
    final Map<String, String> res = new HashMap<>();
    int total = 1000000;
    List<String> keys = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      keys.add("" + i);
    }

//    for (final String k : keys) {
//      exec.submit(new Worker(res, k));
//    }
    for (final String k : keys) {
      exec.submit(new Runnable() {
        @Override
        public void run() {
          synchronized (res) {
            String local_key = k;
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            if (!local_key.equals(k))
              System.err.println("changed!!!");
            res.put(k, Thread.currentThread().getName());
          }
        }
      });
    }

    exec.shutdown();
    exec.awaitTermination(2, TimeUnit.MINUTES);
    System.out.println(res.size());
  }

  static class Worker implements Runnable {
    Map<String, String> res;
    String key;
    public Worker(Map<String, String> res, String key) {
      this.res = res;
      this.key = key;
    }
    @Override
    public void run() {
      synchronized (Worker.class) {
        res.put(key, Thread.currentThread().getName());
      }
    }
  }
}
