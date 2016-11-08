package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by huangql on 4/25/16.
 */
public class HopQueryTest {
    private static final String label = "rycc_relation";

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: titanConf vids hops");
            System.exit(1);
        }
        String titanConf = args[0];
        String vidFile = args[1];
        int hops = Integer.parseInt(args[2]);

        TitanGraph graph = TitanFactory.open(titanConf);
        BufferedReader reader = new BufferedReader(new FileReader(vidFile));
        String id;
        long time = 0, ts;
        int lineCnt = 0, avgVSetSize = 0;
        while ((id = reader.readLine()) != null) {
            Iterator<Vertex> it = graph.getVertices("key", id).iterator();
            if (!it.hasNext()) {
                System.out.println("ERROR: key not found: " + id);
                continue;
            }
            TitanVertex srcV = (TitanVertex) it.next();
            HashSet<Vertex> vSet = new HashSet<>();
            vSet.add(srcV);

            ts = System.currentTimeMillis();
            for (int k = 0; k < hops; k++) {
                TitanMultiVertexQuery mq = graph.multiQuery();
                mq.direction(Direction.BOTH).labels(label);
                mq.addAllVertices(vSet);
                vSet.clear();
                Map<TitanVertex,Iterable<TitanVertex>> results = mq.vertices();
                for (Iterable<TitanVertex> nvs : results.values()) {
                    for (TitanVertex v : nvs) {
                        vSet.add(v);
                    }
                }
            }
            time += System.currentTimeMillis() - ts;

            avgVSetSize += vSet.size();
            lineCnt++;
        }
        System.out.println("average v set size: " + (avgVSetSize / (double)lineCnt));
        System.out.println("time: " + time);
        reader.close();
        graph.shutdown();
    }
}
