package cn.edu.pku.hql.titan;

import cn.edu.pku.hql.titan.mapreduce.ScopaLoaderMR;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by huangql on 4/24/16.
 */
public class EdgeQueryPerf {

    private static Configuration conf = HBaseConfiguration.create();
    private static HConnection conn;
    private static HTableInterface table;
    private static TitanGraph graph;
    private static String edge_ids;
    private static String separator = "\u0001";
    private static byte[] columnFamily = Bytes.toBytes("f");
    private static byte[] columnQualifier = Bytes.toBytes("c");

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: titanConf edgeIdFile isRaw [separator]");
            System.exit(1);
        }
        String titanConf = args[0];
        edge_ids = args[1];
        boolean isRaw = Boolean.parseBoolean(args[2]);
        if (args.length > 3) {
            separator = args[3];
            System.out.println("separator is \"" + separator + "\"");
        }

        conn = HConnectionManager.createConnection(conf);
        table = conn.getTable(Util.getTitanHBaseTableName(titanConf) + ScopaLoaderMR.TABLE_NAME_SUFFIX);
        graph = TitanFactory.open(titanConf);

        if (isRaw) {
            queryRaw();
        } else {
            queryScopa();
        }

        graph.shutdown();
        conn.close();
    }

    private static int queryScopa() throws IOException {
        System.out.println("////////////////////////////////");
        System.out.println("// query test for scopa graph");
        long ts = System.currentTimeMillis();
        long hbaseTime = 0, hts;
        long getVTime = 0, gts;
        long getItTime = 0;
        long whileTime = 0;

        int resCnt = 0;

        BufferedReader reader = new BufferedReader(new FileReader(edge_ids));
        String line;
        while ((line = reader.readLine()) != null) {
            String keys[] = line.split(separator);

            gts = System.currentTimeMillis();
            TitanVertex v1, v2;
            Iterator<Vertex> vit = graph.getVertices("key", keys[0]).iterator();
            v1 = vit.hasNext() ? (TitanVertex)vit.next() : null;
            if (v1 == null) {
                System.err.println("key not found: " + keys[0]);
                System.exit(1);
                //continue;
            }
            vit = graph.getVertices("key", keys[1]).iterator();
            v2 = vit.hasNext() ? (TitanVertex)vit.next() : null;
            if (v2 == null) {
                System.err.println("key not found: " + keys[1]);
                System.exit(1);
                //continue;
            }
            getVTime += System.currentTimeMillis() - gts;

            gts = System.currentTimeMillis();
            Iterator<TitanEdge> it = v1.query().adjacent(v2).labels("rycc_relation").limit(1).titanEdges().iterator();
            getItTime += System.currentTimeMillis() - gts;

            TitanEdge edge = null;
            gts = System.currentTimeMillis();
            while (it.hasNext()) {
                edge = it.next();
            }
            whileTime += System.currentTimeMillis() - gts;
            if (edge == null) continue;
            //System.out.println(v1.getLongId() + " <-> " + v2.getLongId());
            hts = System.currentTimeMillis();
            byte[] startRow = Bytes.toBytes(edge.getId().toString());
            byte[] endRow = Bytes.toBytes(edge.getId().toString());
            endRow[endRow.length - 1] += 1;
            Scan scan = new Scan(startRow, endRow);
            scan.addColumn(columnFamily, columnQualifier);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                resCnt++;
            }
            hbaseTime += System.currentTimeMillis() - hts;
        }
        System.out.println("total time used: " + (System.currentTimeMillis() - ts));
        System.out.println("get vertex time used: " + getVTime);
        System.out.println("get iterator time used: " + getItTime);
        System.out.println("while loop time used: " + whileTime);
        System.out.println("hbase time used: " + hbaseTime);
        System.out.println("selected edges count: " + resCnt);

        reader.close();
        return resCnt;
    }

    private static int queryRaw() throws IOException {
        System.out.println("//////////////////////////////");
        System.out.println("// query test for raw graph");
        long ts = System.currentTimeMillis();
        int resCnt = 0;
        long getVTime = 0, gts;
        long getItTime = 0;
        long whileTime = 0;

        BufferedReader reader = new BufferedReader(new FileReader(edge_ids));
        String line;
        while ((line = reader.readLine()) != null) {
            String keys[] = line.split(separator);

            gts = System.currentTimeMillis();
            TitanVertex v1, v2;
            Iterator<Vertex> vit = graph.getVertices("key", keys[0]).iterator();
            v1 = vit.hasNext() ? (TitanVertex)vit.next() : null;
            if (v1 == null) {
                System.err.println("key not found: " + keys[0]);
                System.exit(1);
                //continue;
            }
            vit = graph.getVertices("key", keys[1]).iterator();
            v2 = vit.hasNext() ? (TitanVertex)vit.next() : null;
            if (v2 == null) {
                System.err.println("key not found: " + keys[1]);
                System.exit(1);
                //continue;
            }
            getVTime += System.currentTimeMillis() - gts;

            gts = System.currentTimeMillis();
            //Iterator<TitanEdge> it = v1.getTitanEdges(Direction.BOTH, graph.getEdgeLabel("rycc_relation")).iterator();
            Iterator<TitanEdge> it = v1.query().adjacent(v2).labels("rycc_relation").titanEdges().iterator();
            //resCnt += v1.query().adjacent(v2).labels("rycc_relation").count();
            getItTime += System.currentTimeMillis() - gts;

            gts = System.currentTimeMillis();
            while (it.hasNext()) {
                TitanEdge edge = it.next();
                resCnt++;
            }
            whileTime += System.currentTimeMillis() - gts;
        }
        System.out.println("total time used: " + (System.currentTimeMillis() - ts));
        System.out.println("get vertex time used: " + getVTime);
        System.out.println("get iterator time used: " + getItTime);
        System.out.println("while loop time used: " + whileTime);
        System.out.println("selected edges count: " + resCnt);

        reader.close();
        return resCnt;
    }
}
