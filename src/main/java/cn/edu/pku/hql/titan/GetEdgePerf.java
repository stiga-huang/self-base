package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import static cn.edu.pku.hql.titan.mapreduce.ScopaLoaderMR.TABLE_NAME_SUFFIX;

/**
 * Created by huangql on 12/8/16.
 */
public class GetEdgePerf {
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("f");
    private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("c");

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: rawTitanConf scopaTitanConf vidFile");
            System.exit(1);
        }
        String rawTitanConf = args[0];
        String scopaTitanConf = args[1];
        String vidFile = args[2];
        getRawEdges(rawTitanConf, vidFile);
        getScopaEdges(scopaTitanConf, vidFile);
    }

    private static void getRawEdges(String rawTitanConf, String vidFile) throws IOException {
        TitanGraph graph = TitanFactory.open(rawTitanConf);
        try (BufferedReader reader = new BufferedReader(new FileReader(vidFile))) {
            String id;
            long totalTime = 0, ts;
            int edgeCnt = 0, vertexCnt = 0;
            while ((id = reader.readLine()) != null) {
                ts = System.currentTimeMillis();
                Iterator<Vertex> it = graph.getVertices("key", id).iterator();
                if (!it.hasNext()) {
                    System.out.println("ERROR: key not found: " + id);
                    continue;
                }
                TitanVertex v = (TitanVertex) it.next();
                vertexCnt++;

                //ts = System.currentTimeMillis();
                for (TitanEdge e : v.getEdges()) {
                    edgeCnt++;
                }
                totalTime += System.currentTimeMillis() - ts;
            }
            System.out.println("Raw:\n"
                    + "totalEdges = " + edgeCnt + ", totalTime = " + totalTime
                    + ", time/edge = " + totalTime / (double) edgeCnt
                    + ", time/vertex = " + totalTime / (double) vertexCnt);
        }
        graph.shutdown();
    }

    private static void getScopaEdges(String scopaTitanConf, String vidFile) throws IOException {
        TitanGraph graph = TitanFactory.open(scopaTitanConf);
        try (HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create())) {
            HTableInterface table = conn.getTable(Util.getTitanHBaseTableName(scopaTitanConf) + TABLE_NAME_SUFFIX);
            try (BufferedReader reader = new BufferedReader(new FileReader(vidFile))) {
                String id;
                long totalTime = 0, ts;
                long totalHBaseTime = 0, hts;
                int edgeCnt = 0, vertexCnt = 0;
                while ((id = reader.readLine()) != null) {
                    ts = System.currentTimeMillis();
                    Iterator<Vertex> it = graph.getVertices("key", id).iterator();
                    if (!it.hasNext()) {
                        System.out.println("ERROR: key not found: " + id);
                        continue;
                    }
                    TitanVertex v = (TitanVertex) it.next();
                    vertexCnt++;

                    //ts = System.currentTimeMillis();
                    for (TitanEdge edge : v.getEdges()) {
                        hts = System.currentTimeMillis();
                        byte[] startRow = Bytes.toBytes(edge.getId().toString());
                        byte[] endRow = Bytes.toBytes(edge.getId().toString());
                        endRow[endRow.length - 1] += 1;
                        Scan scan = new Scan(startRow, endRow);
                        scan.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER);
                        ResultScanner rs = table.getScanner(scan);
                        for (Result r : rs) {
                            edgeCnt++;
                        }
                        totalHBaseTime += System.currentTimeMillis() - hts;
                    }
                    totalTime += System.currentTimeMillis() - ts;
                }
                System.out.println("Scopa:\n"
                        + "totalEdges = " + edgeCnt + ", totalTime = " + totalTime
                        + ", time/edge = " + totalTime / (double) edgeCnt
                        + ", time/vertex = " + totalTime / (double) vertexCnt
                        + ", totalHBaseTime = " + totalHBaseTime);
            }
        }
        graph.shutdown();
    }
}
