package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;

/**
 * Created by huangql on 12/2/16.
 */
public class EdgeCounter {

    private static final Logger LOG = Logger.getLogger(EdgeCounter.class);
    private static long ts;

    private static boolean shouldPrintProgress() {
        if (System.currentTimeMillis() - ts > 1800000) {
            ts = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Args: titanConf [keyName keysFile]");
            System.err.println("\tkeyName\tString of the id property name");
            System.err.println("\tkeysFile\tFile containing keys, one at a line");
            System.exit(1);
        }
        boolean wholeGraph = true;
        String keyName = "", keysFile = "";
        if (args.length >= 3) {
            wholeGraph = false;
            keyName = args[1];
            keysFile = args[2];
        }

        LOG.info("Opening graph");
        TitanGraph graph = TitanFactory.open(args[0]);

        LOG.info("Start to count");
        long vertexCnt = 0, edgeCnt = 0;
        ts = System.currentTimeMillis();
        if (wholeGraph) {
            LOG.info("Counting whold graph");
            for (Vertex v : graph.getVertices()) {
                vertexCnt++;
                edgeCnt += ((TitanVertex) v).getEdgeCount();
                if (shouldPrintProgress()) {
                    LOG.info("vertex count = " + vertexCnt + ", edge count = " + edgeCnt);
                }
            }
        } else {
            LOG.info("Counting with given keys file: " + keysFile);
            BufferedReader reader = new BufferedReader(new FileReader(keysFile));
            String id;
            while ((id = reader.readLine()) != null) {
                Iterator<Vertex> it = graph.query().has(keyName, id).vertices().iterator();
                if (it.hasNext()) {
                    vertexCnt++;
                    edgeCnt += ((TitanVertex)it.next()).getEdgeCount();
                }
                while (it.hasNext()) {
                    LOG.warn("Another vertex found with same id = " + id);
                    vertexCnt++;
                    edgeCnt += ((TitanVertex)it.next()).getEdgeCount();
                }
                if (vertexCnt % 5000 == 0) {
                    LOG.info(vertexCnt + ": key = " + id + ", totalEdges = " + edgeCnt);
                }
            }
        }
        edgeCnt /= 2;

        LOG.info("vertex count = " + vertexCnt);
        LOG.info("edge count = " + edgeCnt);
        graph.shutdown();
    }
}
