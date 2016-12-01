package cn.edu.pku.hql.titan.mapreduce;

import cn.edu.pku.hql.titan.Util;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Iterator;

/**
 * Created by huangql on 4/24/16.
 */
public class ScopaLoaderMR {
    private static final Logger logger = Logger.getLogger(ScopaLoaderMR.class);

    public static final String TITAN_CONF_KEY = "rawDataLoader.titan.conf";
    public static final String LABEL_KEY = "rawDataLoader.label";
    public static final String KEY1_INDEX_KEY = "rawDataLoader.key1.index";
    public static final String KEY2_INDEX_KEY = "rawDataLoader.key2.index";
    public static final String TIME_INDEX_KEY = "rawDataLoader.time.index";

    public static final String TABLE_NAME = "rycc_relation";

    public static class WorkerMapper
            extends Mapper<Object, Text, ImmutableBytesWritable, Put> {

        private static final String timeFormat = "yyyy-MM-dd HH:mm:ss";

        private ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        //private HConnection conn;
        //private HTableInterface table;
        private static final byte[] columnFamily = Bytes.toBytes("f");
        private static final byte[] columnQualifier = Bytes.toBytes("c");

        private TitanGraph graph;
        private String label;
        private int key1Index, key2Index, timeIndex;
        private Counter badLineCount;

        static {
            Util.suppressUselessInfoLogs();
        }

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            graph = TitanFactory.open(conf.get(TITAN_CONF_KEY));
            label = conf.get(LABEL_KEY);
            key1Index = Integer.parseInt(conf.get(KEY1_INDEX_KEY));
            key2Index = Integer.parseInt(conf.get(KEY2_INDEX_KEY));
            timeIndex = Integer.parseInt(conf.get(TIME_INDEX_KEY));

            //Configuration hConf = HBaseConfiguration.create(conf);
            //conn = HConnectionManager.createConnection(hConf);
            //table = conn.getTable(TABLE_NAME);

            badLineCount = context.getCounter("dataloader", "Bad Lines");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(value.toString()))));

            String line;
            int batchCnt = 0;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\u0001");
                if (fields.length <= Math.max(Math.max(key1Index, key2Index), timeIndex)) {
                    logger.error("not enough fields: " + line);
                    badLineCount.increment(1);
                    continue;
                }
                TitanVertex v1, v2;
                Iterator<Vertex> it1 = graph.getVertices("key", fields[key1Index]).iterator();
                Iterator<Vertex> it2 = graph.getVertices("key", fields[key2Index]).iterator();
                if (!it1.hasNext()) {
                    logger.error("key1 not found: " + fields[key1Index]);
                    badLineCount.increment(1);
                    continue;
                }
                if (!it2.hasNext()) {
                    logger.error("key2 not found: " + fields[key2Index]);
                    badLineCount.increment(1);
                    continue;
                }
                v1 = (TitanVertex) it1.next();
                v2 = (TitanVertex) it2.next();
                TitanEdge edge;
                Iterator<TitanEdge> eit = v1.query().adjacent(v2).labels(label).limit(1).titanEdges().iterator();
                if (eit.hasNext()) {
                    edge = eit.next();
                } else {
                    edge = v1.addEdge(label, v2);
                }

                StringBuilder sb = new StringBuilder().append(edge.getId().toString())
                        .append('_').append(System.currentTimeMillis());

                byte[] row = Bytes.toBytes(sb.toString());
                Put put = new Put(row);
                put.add(columnFamily, columnQualifier, Bytes.toBytes(line));
                rowKey.set(row);
                context.write(rowKey, put);

                batchCnt++;
                if (batchCnt == 2000) {
                    graph.commit();
                    batchCnt = 0;
                }
            }
            graph.commit();

            reader.close();
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (graph != null)
                graph.shutdown();
//            if (conn != null)
//                conn.close();
        }
    }

    private static void setupClassPath(Job job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        String titanLibDir = "/user/hadoop/huangql/titanLibs";  // TODO should be an argument
        RemoteIterator<LocatedFileStatus> libIt = fs.listFiles(new Path(titanLibDir), true);
        while (libIt.hasNext()) {
            job.addFileToClassPath(libIt.next().getPath());
        }
    }

    public static boolean createTable(String tableName, Configuration conf) {
        try (HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create(conf))) {
            if (hBaseAdmin.tableExists(tableName)) {
                logger.info("hbase table " + tableName + " exists");
                return true;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));

            HColumnDescriptor cf = new HColumnDescriptor("f");
            cf.setCompressionType(Compression.Algorithm.GZ);
            tableDescriptor.addFamily(cf);

//            // 2 regions per regionServer
//            int count = 2 * hBaseAdmin.getClusterStatus().getServersSize();
//            int startKey = (int)(((1L << 32) - 1) / count);
//            int endKey = startKey * (count - 1);
//            hBaseAdmin.createTable(tableDescriptor, Bytes.toBytes(startKey), Bytes.toBytes(endKey), count);
            byte[][] splitKeys = new byte[26][];
            for (int i = 0; i < 26; i++)
                splitKeys[i] = Bytes.toBytes(('a' + i) << 24);
            hBaseAdmin.createTable(tableDescriptor, splitKeys);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        logger.info("This is ScopaLoaderMR compiled after 9:00PM 2016/5/6");
        Thread.sleep(1000);

        if (args.length < 4) {
            System.out.println("Args: titanConf label indices(key1,key2,time) inputPath [linesPerSplit] [sortByTime]");
            System.exit(1);
        }
        String titanConf = args[0];
        String label = args[1];
        String indices = args[2];
        String inputPath = args[3];
        int linesPerSplit = 1;
        if (args.length > 4)
            linesPerSplit = Integer.parseInt(args[4]);
        boolean sortByTime = false;
        if (args.length > 5)
            sortByTime = Boolean.parseBoolean(args[5]);

        String[] ss = indices.split(",");
        if (ss.length != 3) {
            throw new IllegalArgumentException("error indices: should be key1,key2,time");
        }
        String key1Index = ss[0];
        String key2Index = ss[1];
        String timeIndex = ss[2];

        // make edge schema
        TitanGraph graph = TitanFactory.open(titanConf);
        TitanManagement mgnt = graph.getManagementSystem();
        if (!mgnt.containsPropertyKey("value"))
            mgnt.makePropertyKey("value").dataType(String.class).make();
        PropertyKey time;
        if (!mgnt.containsPropertyKey("time"))
            time = mgnt.makePropertyKey("time").dataType(Long.class).make();
        else
            time = mgnt.getPropertyKey("time");
        if (!mgnt.containsEdgeLabel("rycc_relation")) {
            if (sortByTime) {
                System.out.println("edges are sorted by time");
                mgnt.makeEdgeLabel("rycc_relation").signature(time).make();
            } else {
                System.out.println("edges are not sorted by time");
                mgnt.makeEdgeLabel("rycc_relation").make();
            }
        }
        mgnt.commit();
        graph.shutdown();
        // create hbase table;
        Configuration conf = HBaseConfiguration.create();
        String zookeeperQuorumKey = "hbase.zookeeper.quorum";
        String zookeeperQuorum = "daim209";
        if (!zookeeperQuorum.equals(conf.get(zookeeperQuorumKey))) {
            logger.error("Wrong " + zookeeperQuorumKey + ": " + conf.get(zookeeperQuorumKey));
            conf.set(zookeeperQuorumKey, zookeeperQuorum);
        }
        createTable(TABLE_NAME, conf);

        Job job = Job.getInstance(conf, "scopa titan edges loader");

        job.setJarByClass(ScopaLoaderMR.class);
        job.setMapperClass(WorkerMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //job.setNumReduceTasks(0);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(inputPath));
        NLineInputFormat.setNumLinesPerSplit(job, linesPerSplit);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/scopaBulkLoading"));
        //job.setOutputFormatClass(NullOutputFormat.class);

        // make sure every worker running uniquely
        job.setSpeculativeExecution(false);
        // default task timeout is 10min, set it to 0 to disable timeout
        //job.getConfiguration().set("mapreduce.task.timeout", "0");

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
        // It will be closed When JVM exit.
        // These tmp files will be deleted at that time.
        job.getConfiguration().set(TITAN_CONF_KEY, baseName);
        job.getConfiguration().set(LABEL_KEY, label);
        job.getConfiguration().set(KEY1_INDEX_KEY, key1Index);
        job.getConfiguration().set(KEY2_INDEX_KEY, key2Index);
        job.getConfiguration().set(TIME_INDEX_KEY, timeIndex);

        // setup reducer by HFileOutputFormat2
        try (HTable table = new HTable(conf, TABLE_NAME)) {
            HFileOutputFormat2.configureIncrementalLoad(job, table);
        }

        job.waitForCompletion(true);

        logger.info("finally, counting vertices and edges...");
        graph = TitanFactory.open(titanConf);
        int vertexCnt = 0, edgeCnt = 0;
        for (Vertex v : graph.getVertices()) {
            vertexCnt++;
            edgeCnt += ((TitanVertex)v).getEdgeCount();
        }
        edgeCnt /= 2;
        logger.info("vertex count = " + vertexCnt);
        logger.info("edge count = " + edgeCnt);
        graph.shutdown();
    }
}
