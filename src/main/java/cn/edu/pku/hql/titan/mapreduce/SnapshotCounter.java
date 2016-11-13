package cn.edu.pku.hql.titan.mapreduce;

import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.stats.NumberUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

/**
 * This counter read titan graph data from HBase snapshot
 * and count vertices and edges by MapReduce.
 *
 * Created by huangql on 11/12/16.
 */
public class SnapshotCounter implements Tool {

    public static final String TITAN_CONF_KEY = "snapshot.counter.titan.conf";
    private Configuration conf;

    enum TitanCounters {
        VERTEX_COUNT,
        EDGE_COUNT
    }

    // Copy from com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore$HBaseGetter
    private static class HBaseGetter implements
            StaticArrayEntry.GetColVal<Map.Entry<byte[], NavigableMap<Long, byte[]>>, byte[]> {

        private final EntryMetaData[] schema;

        private HBaseGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return element.getKey();
        }

        @Override
        public byte[] getValue(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return element.getValue().lastEntry().getValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<byte[], NavigableMap<Long, byte[]>> element) {
            return schema;
        }

        @Override
        public Object getMetaData(Map.Entry<byte[], NavigableMap<Long, byte[]>> element, EntryMetaData meta) {
            switch(meta) {
                case TIMESTAMP:
                    return element.getValue().lastEntry().getKey();
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    public static class CounterMap extends TableMapper<NullWritable, NullWritable> {

        private StandardTitanGraph graph;
        private StandardTitanTx tx;
        // For stores that preserve key order (such as HBase and Cassandra), cluster.partition default to true.
        // References:
        // http://s3.thinkaurelius.com/docs/titan/0.5.4/titan-config-ref.html
        // https://github.com/thinkaurelius/titan/blob/0.5.4/titan-core/src/main/java/com/thinkaurelius/titan/graphdb/configuration/GraphDatabaseConfiguration.java#L622
        // https://github.com/thinkaurelius/titan/blob/0.5.4/titan-core/src/main/java/com/thinkaurelius/titan/graphdb/database/idassigner/VertexIDAssigner.java#L75
        // https://github.com/thinkaurelius/titan/blob/0.5.4/titan-core/src/main/java/com/thinkaurelius/titan/graphdb/configuration/GraphDatabaseConfiguration.java#L630
        private IDManager idManager = new IDManager(NumberUtil.getPowerOf2(64L));
        private HBaseGetter entryGetter = new HBaseGetter(new EntryMetaData[]{EntryMetaData.TIMESTAMP});
        private int vertexCount = 0;
        private int edgeCount = 0;

        public void setup(Context context) throws IOException, InterruptedException {
            graph = (StandardTitanGraph) TitanFactory.open(
                    context.getConfiguration().get(TITAN_CONF_KEY));
            tx = (StandardTitanTx) graph.newTransaction();
        }

        public void map(ImmutableBytesWritable key, Result value,
                        Context context) throws IOException, InterruptedException {
            long vid = idManager.getKeyID(new StaticArrayBuffer(key.get()));
            if (!IDManager.VertexIDType.NormalVertex.is(vid))
                return;
            vertexCount++;

            // See com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore#getHelper
            EntryList entryList = StaticArrayEntryList.ofBytes(
                    value.getMap().get(Bytes.toBytes("e")).entrySet(),
                    entryGetter);
            for (Entry entry : entryList) {
                RelationCache relation = graph.getEdgeSerializer().readRelation(entry, false, tx);
                RelationType type = tx.getExistingRelationType(relation.typeId);
                if (type.isEdgeLabel())
                    edgeCount++;
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(TitanCounters.VERTEX_COUNT).increment(vertexCount);
            context.getCounter(TitanCounters.EDGE_COUNT).increment(edgeCount);
            if (graph != null) {
                graph.shutdown();
            }
        }
    }

    private static void setupClassPath(Job job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        String titanLibDir = "/user/hadoop/huangql/titanLibs";  // TODO should be an argument
        System.out.println("Using titan libs in HDFS path: " + titanLibDir);
        RemoteIterator<LocatedFileStatus> libIt = fs.listFiles(new Path(titanLibDir), true);
        while (libIt.hasNext()) {
            job.addFileToClassPath(libIt.next().getPath());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Args: snapshotName titanConf");
            return 1;
        }
        String snapshotName = args[0];
        String titanConf = args[1];

        Job job = Job.getInstance(getConf(), "Titan vertices & edges counter");
        job.setJarByClass(SnapshotCounter.class);
        job.setMapperClass(CounterMap.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TableSnapshotInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableSnapshotMapperJob(
                snapshotName,
                new Scan().addFamily(Bytes.toBytes("e")),
                CounterMap.class,
                NullWritable.class,
                NullWritable.class,
                job,
                false,
                new Path("/tmp/snapshot_counter" + new Random().nextInt()));

        setupClassPath(job);
        // upload titanConf and add to distributed cache
        File file = new File(titanConf);
        if (!file.exists())
            throw new FileNotFoundException(titanConf);
        String baseName = file.getName();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(file.toURI());
        Path dst = new Path("/tmp/ScopaLoaderMR/" + baseName);
        fs.copyFromLocalFile(src, dst);
        job.addCacheFile(dst.toUri());
        fs.deleteOnExit(dst);   // DO Not close this fs!

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        InputStream in = SnapshotCounter.class.getClassLoader().getResourceAsStream("hbase-site.xml");
        if (in == null) {
            System.err.println("hbase-site.xml not found in classpath");
            System.exit(1);
        } else {
            in.close();
        }

        Configuration conf = HBaseConfiguration.create();
        if (conf.get(HConstants.HBASE_DIR) == null) {
            System.err.println("hbase configs not set");
            System.exit(1);
        }
        System.out.println("hbase root dir: " + conf.get(HConstants.HBASE_DIR));

        ToolRunner.run(conf, new SnapshotCounter(), args);
    }
}
