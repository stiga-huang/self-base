package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * Created by huangql on 1/14/16.
 */
public class InstanceTest {

    public static TitanGraph openTitanGraph() {
        TitanFactory.Builder config = TitanFactory.build();
        config.set("storage.backend", "hbase");
        config.set("storage.hostname", "localhost");
        config.set("storage.hbase.table", "instanceTest");
        return config.open();
    }

    public static void printOpenInstances(TitanGraph g) {
        for (String name : g.getManagementSystem().getOpenInstances()) {
            System.out.println(name);
        }
    }

    public static void main(String[] args) {

        TitanGraph g = openTitanGraph();

        printOpenInstances(g);
        try {
            while (true)
                Thread.sleep(1000);
        } catch (Exception e) { // can't catch "kill -9"
            e.printStackTrace();
        } finally {
            g.shutdown();
        }

        // kill this process outside:
        // $ kill -9 `jps | grep AppMain | cut -f 1 -d\ `
    }
}
