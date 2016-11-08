package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Created by huangql on 3/28/16.
 *
 * Titan 事务测试： 事务是原子的，但不能保证一致性。
 * 如果有两个并发事务对同一个值做追加，最终结果是不确定的
 */
public class TransactionTest {

    public static void main(String[] args) throws InterruptedException {
        final TitanGraph g = TitanFactory.open("inmemory");

        TitanManagement management = g.getManagementSystem();
        management.makePropertyKey("tx").dataType(String.class).make();
        management.makePropertyKey("age").dataType(Integer.class).make();
        management.commit();

        TitanVertex v = g.addVertex();
        v.setProperty("tx", Thread.currentThread().getName());
        v.setProperty("age", 10);
        final long vid = v.getLongId();
        g.commit();

        Runnable worker = new Runnable() {
            @Override
            public void run() {
                String tName = Thread.currentThread().getName();

                TitanVertex v = g.getVertex(vid);
                v.setProperty("tx", tName);
                int age = v.getProperty("age");
                v.setProperty("age", age + 10);
                System.out.println("at first, age = " + age + " in " + tName);
                System.out.println("finished setting property, " + tName + " start to sleep 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                g.commit();
                System.out.println(tName + " committed");
            }
        };
        Thread t1 = new Thread(worker);
        Thread t2 = new Thread(worker);

        System.out.println("before threads start:");
        System.out.println(g.getVertex(vid).getProperty("tx"));
        System.out.println(g.getVertex(vid).getProperty("age"));
        g.commit();

        t1.start();
        t2.start();

        Thread.sleep(1000);

        t1.join();
        t2.join();

        System.out.println("after threads run:");
        System.out.println(g.getVertex(vid).getProperty("tx"));
        System.out.println(g.getVertex(vid).getProperty("age"));

        g.shutdown();
    }
}
