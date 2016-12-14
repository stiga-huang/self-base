package cn.edu.pku.hql.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.hbase.CellUtil;
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

    enum ExecuteType {
        SCOPA, RAW, BOTH_RAW_FIRST, BOTH_SCOPA_FIRST
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("Usage: rawTitanConf scopaTitanConf vidFile type\n" +
                    "\ttype can be raw,scopa,both_raw_first,both_scopa_first");
            System.exit(1);
        }
        String rawTitanConf = args[0];
        String scopaTitanConf = args[1];
        String vidFile = args[2];
        String execType = args[3];
        if (ExecuteType.SCOPA.name().equalsIgnoreCase(execType)) {
            getScopaEdges(scopaTitanConf, vidFile);
        } else if (ExecuteType.RAW.name().equalsIgnoreCase(execType)) {
            getRawEdges(rawTitanConf, vidFile);
        } else if (ExecuteType.BOTH_RAW_FIRST.name().equalsIgnoreCase(execType)) {
            getRawEdges(rawTitanConf, vidFile);
            getScopaEdges(scopaTitanConf, vidFile);
        } else if (ExecuteType.BOTH_SCOPA_FIRST.name().equalsIgnoreCase(execType)) {
            getScopaEdges(scopaTitanConf, vidFile);
            getRawEdges(rawTitanConf, vidFile);
        } else {
            System.err.println("Unknown execution type!");
        }
    }

    private static void getRawEdges(String rawTitanConf, String vidFile) throws IOException {
        TitanGraph graph = TitanFactory.open(rawTitanConf);
        try (BufferedReader reader = new BufferedReader(new FileReader(vidFile))) {
            String id;
            long totalTime = 0, ts;
            long totalSerTime = 0, sts;
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
                    sts = System.currentTimeMillis();
                    long timeValue = e.getProperty("time");
                    String content = e.getProperty("value");
                    totalSerTime += System.currentTimeMillis() - sts;

                    edgeCnt++;
                }
                totalTime += System.currentTimeMillis() - ts;
            }
            System.out.println(String.format("Raw:\ttotalEdges = %d, totalTime = %d"
                    + ", time/edge = %2f, time/vertex = %f" +
                    ", totalSerTime = %d, serTime/edge = %f",
                    edgeCnt, totalTime, totalTime / (double) edgeCnt, totalTime / (double) vertexCnt,
                    totalSerTime, totalSerTime / (double) edgeCnt));
        }
        graph.shutdown();
    }

    private static void getScopaEdges(String scopaTitanConf, String vidFile) throws IOException {
        TitanGraph graph = TitanFactory.open(scopaTitanConf);
        try (HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create())) {
            try (HTableInterface table = conn.getTable(Util.getTitanHBaseTableName(scopaTitanConf)
                    + TABLE_NAME_SUFFIX)) {
//                Scan sc = new Scan();
//                sc.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER);
//                System.out.println("batch: " + sc.getBatch());
//                System.out.println("caching: " + sc.getCaching());
//                System.out.println("cacheBlocks: " + sc.getCacheBlocks());
//                System.out.println("maxResultSize: " + sc.getMaxResultSize());
//                System.out.println("maxResultsPerColumnFamily: " + sc.getMaxResultsPerColumnFamily());
//                table.getScanner(sc).close();

                try (BufferedReader reader = new BufferedReader(new FileReader(vidFile))) {
                    String id;
                    long totalTime = 0, ts;
                    long totalSerTime = 0, sts;
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
                            scan.setCaching(Integer.MAX_VALUE);
                            try (ResultScanner rs = table.getScanner(scan)) {
                                for (Result r : rs) {
                                    sts = System.currentTimeMillis();
                                    String row = new String(r.getRow());
                                    long timeValue = Long.parseLong(row.substring(row.lastIndexOf('_') + 1));
                                    String content = new String(CellUtil.cloneValue(r.getColumnLatestCell(
                                            COLUMN_FAMILY, COLUMN_QUALIFIER)));
                                    totalSerTime += System.currentTimeMillis() - sts;

                                    edgeCnt++;
                                }
                            }
                            totalHBaseTime += System.currentTimeMillis() - hts;
                        }
                        totalTime += System.currentTimeMillis() - ts;
                    }
                    System.out.println(String.format("Scopa:\ttotalEdges = %d, totalTime = %d" +
                            ", time/edge = %2f, time/vertex = %f" +
                            ", totalSerTime = %d, serTime/edge = %f" +
                            ", totalHBaseTime = %d",
                            edgeCnt, totalTime, totalTime / (double) edgeCnt, totalTime / (double) vertexCnt,
                            totalSerTime, totalSerTime / (double) edgeCnt, totalHBaseTime));
                }
            }
        }
        graph.shutdown();
    }
}
