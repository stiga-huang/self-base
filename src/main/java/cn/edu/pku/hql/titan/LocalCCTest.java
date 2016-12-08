package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Test for calculating local clustering coefficient
 *
 * Created by huangql on 12/8/16.
 */
public class LocalCCTest {

    private static HashSet<Vertex> getNeighbors(Vertex src) {
        HashSet<Vertex> rs = new HashSet<>();
        for (Vertex v : src.getVertices(Direction.BOTH)) {
            rs.add(v);
        }
        return rs;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: titanConf vids");
            System.exit(1);
        }
        String titanConf = args[0];
        String vidFile = args[1];
        TitanGraph graph = TitanFactory.open(titanConf);
        BufferedReader reader = new BufferedReader(new FileReader(vidFile));
        String id;
        long totalTime = 0, ts;
        int lineCnt = 0;
        double avgCC = 0;   // average clustering coefficient
        while ((id = reader.readLine()) != null) {
            Iterator<Vertex> it = graph.getVertices("key", id).iterator();
            if (!it.hasNext()) {
                System.out.println("ERROR: key not found: " + id);
                continue;
            }
            TitanVertex v = (TitanVertex) it.next();

            ts = System.currentTimeMillis();
            double localCC = 0;
            Set<Vertex> nbs = getNeighbors(v);
            int n = nbs.size();
            if (n > 1) {
                int adjCnt = 0;
                for (Vertex vi : nbs) {
                    for (Vertex vj : getNeighbors(vi)) {
                        if (nbs.contains(vj))
                            adjCnt++;
                    }
                }
                localCC = adjCnt / (double)(n*(n-1));
            }
            //System.out.println(id + "\t" + localCC);
            avgCC += localCC;
            totalTime += System.currentTimeMillis() - ts;

            lineCnt++;
        }
        if (lineCnt > 0)    avgCC /= lineCnt;
        System.out.println("average cc: " + avgCC);
        if (lineCnt > 0)
            System.out.println("average time: " + totalTime / (double)lineCnt);
        reader.close();
        graph.shutdown();
    }
}
