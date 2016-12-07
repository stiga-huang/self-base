package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Created by huangql on 12/6/16.
 */
public class CacheConsistenceTest {
    private static final String EDGE_LABEL = "relation";

    public static void main(String[] args) {
        TitanGraph g1 = TitanFactory.open(args[0]);
        TitanGraph g2 = TitanFactory.open(args[0]);

        TitanManagement mgnt = g1.getManagementSystem();
        if (!mgnt.containsEdgeLabel(EDGE_LABEL))
            mgnt.makeEdgeLabel(EDGE_LABEL).make();
        mgnt.commit();

        long vid1 = g1.addVertex().getLongId();
        long vid2 = g1.addVertex().getLongId();
        g1.commit();

        TitanVertex v11 = g1.getVertex(vid1);
        TitanVertex v12 = g1.getVertex(vid2);
        TitanVertex v21 = g2.getVertex(vid1);
        TitanVertex v22 = g2.getVertex(vid2);

        v11.addEdge(EDGE_LABEL, v12);
        v21.addEdge(EDGE_LABEL, v22);
        g1.commit();
        g2.commit();

        System.out.println(g1.getVertex(vid1).getEdgeCount());
        System.out.println(v11 == v21);
        System.out.println(v11.equals(v21));

        g1.shutdown();
        g2.shutdown();
    }
}
