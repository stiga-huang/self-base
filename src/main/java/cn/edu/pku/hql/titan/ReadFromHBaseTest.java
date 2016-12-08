package cn.edu.pku.hql.titan;

import cn.edu.pku.hql.titan.mapreduce.SnapshotCounter;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by huangql on 12/8/16.
 */
public class ReadFromHBaseTest {
    public static void main (String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Args: titanConf vid");
            System.exit(1);
        }
        String titanConf = args[0];
        long vid = Long.parseLong(args[1]);

        StandardTitanGraph graph = (StandardTitanGraph) TitanFactory.open(titanConf);
        TitanVertex v = graph.getVertex(vid);
        if (v == null) {
            System.err.println("Vertex id not found: " + vid);
            System.exit(1);
        }
        System.out.println("vid = " + vid);
        System.out.println("----- from titan -----");
        int propCount = 0;
        for (TitanProperty p : v.getProperties()) {
            System.out.println(propCount++ + ": " + p.toString() + "\t" + p.getLongId());
        }
        int edgeCount = 0;
        for (TitanEdge e : v.getEdges()) {
            System.out.println(edgeCount++ + ": " + e.toString() + "\t" + e.getLongId());
        }

        StaticBuffer sb = graph.getIDManager().getKey(vid);
        byte[] rowKey = sb.getBytes(0, sb.length());
        SnapshotCounter.HBaseGetter entryGetter = new SnapshotCounter.HBaseGetter();
        HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create());
        HTableInterface table = conn.getTable(Util.getTitanHBaseTableName(titanConf));
        Get getReq = new Get(rowKey);
        Result rs = table.get(getReq);
        EntryList entryList = StaticArrayEntryList.ofBytes(
                rs.getMap().get(Bytes.toBytes("e")).entrySet(),
                entryGetter);
        StandardTitanTx tx = (StandardTitanTx) graph.newTransaction();

        System.out.println("----- from hbase -----");
        edgeCount = 0;
        for (Entry entry : entryList) {
            RelationCache relation = graph.getEdgeSerializer().readRelation(entry, false, tx);
            RelationType type = tx.getExistingRelationType(relation.typeId);
//                if (type.isEdgeLabel() && !graph.getIDInspector().isEdgeLabelId(relation.relationId))
//                    edgeCount++;
            System.out.println(edgeCount++ + ": " + relation.toString()
                    + ", isEdgeLabel = " + type.isEdgeLabel()
                    + ", isEdgeLabelId = " + graph.getIDInspector().isEdgeLabelId(relation.relationId)
                    + ", isSystemRelationTypeId = " + graph.getIDInspector().isSystemRelationTypeId(type.getLongId()));
        }

        graph.shutdown();
    }
}
