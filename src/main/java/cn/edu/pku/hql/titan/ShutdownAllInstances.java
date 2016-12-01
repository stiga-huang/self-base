package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

/**
 * This class is used to shutdown all instances of a graph
 *
 * Created by huangql on 12/1/16.
 */
public class ShutdownAllInstances {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Args: titanConf");
            System.exit(1);
        }

        StandardTitanGraph g = (StandardTitanGraph) TitanFactory.open(args[0]);
        TitanManagement mgnt = g.getManagementSystem();
        System.out.println("Open instances: " + mgnt.getOpenInstances());
        for (String instance : mgnt.getOpenInstances()) {
            if (instance.endsWith("(current)"))
                continue;
            mgnt.forceCloseInstance(instance);
            System.out.println("Shutdown " + instance);
        }
        g.shutdown();
    }
}
