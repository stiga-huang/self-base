package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by huangql on 5/8/16.
 */
public class VertexLoader {
    private static final Logger logger = Logger.getLogger(VertexLoader.class);

    public static void main(String[] args) throws IOException {
        logger.info("This is VertexLoader compiled after 10:00AM 2016/5/8");
        if (args.length < 4) {
            System.out.println("Args: titanConf label Indices(key,xm) inputPath [separator]");
            System.exit(1);
        }
        String titanConf = args[0];
        String label = args[1];
        String indices = args[2];
        String inputPath = args[3];
        String separator = "\u0001";
        if (args.length > 4)
            separator = args[4];

        String[] ss = indices.split(",");
        if (ss.length != 2) {
            throw new IllegalArgumentException("error indices: should be index of key,xm");
        }
        int keyIndex = Integer.parseInt(ss[0]);
        int xmIndex = Integer.parseInt(ss[1]);

        TitanGraph graph = TitanFactory.open(titanConf);
        TitanManagement mgnt = graph.getManagementSystem();
        if (!mgnt.containsVertexLabel(label))
            mgnt.makeVertexLabel(label).make();
        if (!mgnt.containsPropertyKey("xm"))
            mgnt.makePropertyKey("xm").dataType(String.class).make();
        PropertyKey key;
        if (!mgnt.containsPropertyKey("key"))
            key = mgnt.makePropertyKey("key").dataType(String.class).make();
        else
            key = mgnt.getPropertyKey("key");
        if (!mgnt.containsGraphIndex("byKey")) {
            mgnt.buildIndex("byKey", Vertex.class).addKey(key).unique().buildCompositeIndex();
        }
        mgnt.commit();

        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        int batchCnt = 0, totalCnt = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            String f[] = line.split(separator);
            TitanVertex v = graph.addVertexWithLabel(label);
            v.addProperty("key", f[keyIndex]);
            v.addProperty("xm", f[xmIndex]);
            batchCnt++;

            if (batchCnt >= 20000) {
                graph.commit();
                totalCnt += batchCnt;
                batchCnt = 0;
                logger.info("committed: " + totalCnt);
            }
        }
        graph.commit();
        reader.close();

        graph.shutdown();
    }
}
