package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.Iterator;

/**
 * Created by huangql on 4/23/16.
 */
public class EdgeQueryTest {
    private static void listEdges(TitanVertex v) {
        for (TitanEdge e : v.getEdges()) {
            System.out.println(e.toString());
        }
    }
    public static void main(String[] args) {
        TitanGraph graph = TitanFactory.open("inmemory");
        graph.getManagementSystem().makeEdgeLabel("myLabel").make();

        TitanVertex v1, v2, v3;
        long id1, id2, id3;
        v1 = graph.addVertex();
        v2 = graph.addVertex();
        v3 = graph.addVertex();
        id1 = v1.getLongId();
        id2 = v2.getLongId();
        id3 = v3.getLongId();
        System.out.println("id1 = " + id1);
        System.out.println("id2 = " + id2);
        System.out.println("id3 = " + id3);

        v1.addEdge("myLabel", v2);
        v1.addEdge("myLabel", v2);
        v1.addEdge("myLabel", v3);
        //v2.addEdge("myLabel", v3);
        listEdges(v1);
        listEdges(v2);
        listEdges(v3);

        System.out.println(v1.getEdgeCount());

        graph.commit();

        v1 = graph.getVertex(id1);
        v2 = graph.getVertex(id2);
        v3 = graph.getVertex(id3);
//        listEdges(v1);
//        listEdges(v2);
//        listEdges(v3);

        Iterator<TitanEdge> it = v1.getTitanEdges(Direction.BOTH, graph.getEdgeLabel("myLabel")).iterator();
        while (it.hasNext()) {
            TitanEdge edge = it.next();
            if (edge.getOtherVertex(v1).equals(v2)) {
                System.out.println("hit one");
            }
        }

        //it = v1.query().adjacent(v2).labels("myLabel").titanEdges().iterator();
        it = v1.query().adjacent(v2).titanEdges().iterator();
        while (it.hasNext()) {
            TitanEdge edge = it.next();
            //if (!edge.getOtherVertex(v1).equals(v2)) {
                System.out.println(edge.getOtherVertex(v1));
            //}
        }
        //System.out.println(v3);
        graph.shutdown();
    }
}
