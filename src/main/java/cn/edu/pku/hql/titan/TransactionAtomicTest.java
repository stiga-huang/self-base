package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Created by huangql on 3/28/16.
 */
public class TransactionAtomicTest {

    public static void main(String[] args) throws InterruptedException {
        final TitanGraph g = TitanFactory.open("inmemory");

        TitanManagement management = g.getManagementSystem();
        PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
        PropertyKey age = management.makePropertyKey("age").dataType(Integer.class).make();
        management.setConsistency(age, ConsistencyModifier.LOCK);
        management.commit();

        TitanVertex v = g.addVertex();
        v.setProperty("name", Thread.currentThread().getName());
        v.setProperty("age", 10);
        final long vid = v.getLongId();
        g.commit();

        Runnable worker = new Runnable() {
            @Override
            public void run() {
                String tName = Thread.currentThread().getName();

                TitanVertex v = g.getVertex(vid);
                v.setProperty("name", tName);
                // 如果在此commit，则修改可以生效，因为name属性并没有一致性约束
                //g.commit();

                int age = v.getProperty("age");
                v.setProperty("age", age + 10);
                System.out.println("at first, age = " + age + " in " + tName);
                System.out.println("finished setting property, " + tName + " start to sleep 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                g.commit(); // 其中一个线程将commit失败，因为另一个线程获得了age的修改锁
                            // 根据事务的原子性，commit失败则对name的修改也失效
                System.out.println(tName + " committed");
            }
        };
        Thread t1 = new Thread(worker);
        Thread t2 = new Thread(worker);

        System.out.println("before threads start:");
        System.out.println(g.getVertex(vid).getProperty("name"));
        System.out.println(g.getVertex(vid).getProperty("age"));
        g.commit();

        t1.start();
        t2.start();

        Thread.sleep(1000);

        t1.join();
        t2.join();

        System.out.println("after threads run:");
        System.out.println(g.getVertex(vid).getProperty("name"));
        System.out.println(g.getVertex(vid).getProperty("age"));

        g.shutdown();
    }
}
