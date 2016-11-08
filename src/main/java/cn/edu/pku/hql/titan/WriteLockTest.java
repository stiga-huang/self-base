package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by huangql on 1/15/16.
 */
public class WriteLockTest {

    public static TitanGraph getHBaseGraph() {
        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("storage.backend", "hbase");
        conf.setProperty("storage.hostname", "localhost");
        return TitanFactory.open(conf);
    }

    public static TitanGraph getMemoryGraph() {
        return TitanFactory.open("inmemory");
    }

    public static void makeSchema(TitanGraph g) {
        TitanManagement mgmt = g.getManagementSystem();

        if (!mgmt.containsVertexLabel("human"))
            mgmt.makeVertexLabel("human").make();

        if (!mgmt.containsEdgeLabel("shared"))
            mgmt.makeEdgeLabel("shared").make();

//        if (!mgmt.containsPropertyKey("name")) {
//            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
//            TitanGraphIndex namei = mgmt.buildIndex("name",Vertex.class).addKey(name).unique().buildCompositeIndex();
//            mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
//        }

        if (!mgmt.containsPropertyKey("account")) {
            PropertyKey account = mgmt.makePropertyKey("account").dataType(Integer.class).cardinality(Cardinality.LIST).make();
            mgmt.setConsistency(account, ConsistencyModifier.FORK);
        }

        mgmt.commit();
    }

    public static Object loadInitGraph(TitanGraph g) {
        TitanVertex Alice = g.addVertexWithLabel("human");
        Alice.setProperty("name", "Alice");
        Alice.addProperty("account", 1000);

        TitanVertex Bob = g.addVertexWithLabel("human");
        Bob.setProperty("name", "Bob");
        Bob.addProperty("account", 2000);

        PropertyKey account = g.getPropertyKey("account");

        TitanEdge e = Alice.addEdge("shared", Bob);
        e.setProperty(account, 500);
        Object eid = e.getId();
        g.commit();

        return eid;
    }

    public static void main(String[] args) throws Exception {
        //final TitanGraph g = getHBaseGraph();
        final TitanGraph g = getMemoryGraph();
        makeSchema(g);
        final Object edgeId = loadInitGraph(g);

        final Object lock = new Object();

        Runnable adder = new Runnable() {
            @Override
            public void run() {
                String threadName = Thread.currentThread().getName();
                synchronized (lock) {
                    try {
                        System.out.println(threadName + " start to wait");
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(threadName + " start to write");
                g.getEdge(edgeId).setProperty("account", 1000);
                g.commit();
//                Iterator it = g.query().has("name", "Bob").vertices().iterator();
//                if (it.hasNext()) {
//                    TitanVertex Bob = (TitanVertex) it.next();
//                    String str = "";
//                    for (TitanProperty p : Bob.getProperties("account")) {
//                        str += "|" + p.getValue();
//                    }
//                    System.out.println(threadName + " see accounts: " + str);
//                    Bob.addProperty("account", 10000);
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    g.commit();
//                    //System.out.println(threadName + " read " + account + ", write " + newAccount);
//                }
            }
        };
        Thread t1 = new Thread(adder);
        Thread t2 = new Thread(adder);

        t1.start();
        t2.start();

        Thread.sleep(100);
        synchronized (lock) {
            lock.notifyAll();
        }

        t1.join();
        t2.join();

        Iterator it = g.query().has("name", "Bob").vertices().iterator();
        if (it.hasNext()) {
            System.out.println("final account of Edge: " + g.getEdge(edgeId).getProperty("account"));
//            TitanVertex Bob = (TitanVertex) it.next();
//            ArrayList<Integer> accounts = (ArrayList<Integer>)Bob.getProperty("account");
//            for (Integer account : accounts)
//                System.out.println("final account of Bob: " + account);
//            g.commit();
        }

        g.shutdown();
    }
}
