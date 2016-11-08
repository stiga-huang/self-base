package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;

/**
 * Created by huangql on 3/28/16.
 */
public class MultiThreadConsistenceTest {

    public static void main(String[] args) throws InterruptedException {
        final TitanGraph g = TitanFactory.open("inmemory");

        TitanManagement management = g.getManagementSystem();
        PropertyKey prop = management.makePropertyKey("name").dataType(String.class).make();
        TitanGraphIndex namei = management.buildIndex("nameIndex", Vertex.class).addKey(prop)
                .unique().buildCompositeIndex();
        // 若不加下面这句，则两个线程都能提交成功
        management.setConsistency(namei, ConsistencyModifier.LOCK);
        // 若使用FORK，会生成新的点
        // management.setConsistency(namei, ConsistencyModifier.FORK);

        management.commit();

        Runnable worker = new Runnable() {
            @Override
            public void run() {
                g.addVertex().setProperty("name", "Bob");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                g.commit();
            }
        };

        Thread t1 = new Thread(worker);
        Thread t2 = new Thread(worker);

        t1.start();
        t2.start();

        Thread.sleep(1000);

        t1.join();
        t2.join();

        // list all vertices
        System.out.println("list all vertices:");
        int vertexCount = 0;
        for (Vertex v : g.getVertices()) {
            System.out.println("vid = " + v.getId());
            System.out.println(v.getProperty("name"));
            vertexCount++;
        }
        System.out.println("vertexCount = " + vertexCount);

        // find name Bob
        System.out.println("\nfind name Bob:");
        Iterator iter = g.query().has("name", "Bob").vertices().iterator();
        while (iter.hasNext()) {
            Vertex v = (Vertex)iter.next();
            System.out.println("vid = " + v.getId());
            System.out.println(v.getProperty("name"));
        }
        g.shutdown();
    }
}
