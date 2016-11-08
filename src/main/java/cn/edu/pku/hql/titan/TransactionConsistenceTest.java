package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Created by huangql on 3/28/16.
 *
 * 属性的一致性需要显示地定义，如：
 *   management.setConsistency(age, ConsistencyModifier.LOCK);
 * 并发事务操作同一个有一致性约束的属性值，只有一个事务会成功，
 * 其它事务的重试需要在用户代码中编写！
 */
public class TransactionConsistenceTest {

    public static void main(String[] args) throws InterruptedException {
        final TitanGraph g = TitanFactory.open("inmemory");

        TitanManagement management = g.getManagementSystem();
        PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
        PropertyKey age = management.makePropertyKey("age").dataType(Integer.class).make();
        management.setConsistency(name, ConsistencyModifier.LOCK);
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
                int cnt = 0;
                while (true) {
                    try {
                        TitanVertex v = g.getVertex(vid);
                        v.setProperty("name", tName);
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
                        break;
                    } catch (TitanException e) {
                        cnt++;
                        System.out.println(tName + " tried " + cnt + " times");
                    }
                }
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
