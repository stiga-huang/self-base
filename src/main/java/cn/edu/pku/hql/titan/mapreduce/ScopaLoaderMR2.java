package cn.edu.pku.hql.titan.mapreduce;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * Created by huangql on 4/24/16.
 */
public class ScopaLoaderMR2 {
    private static final Logger logger = Logger.getLogger(ScopaLoaderMR2.class);

    public static final String TITAN_CONF_KEY = "scopaDataLoader.titan.conf";
    public static final String LABEL_KEY = "scopaDataLoader.label";
    public static final String KEY1_INDEX_KEY = "scopaDataLoader.key1.index";
    public static final String KEY2_INDEX_KEY = "scopaDataLoader.key2.index";
    public static final String TIME_INDEX_KEY = "scopaDataLoader.time.index";
    public static final String EDGE_TIMES_KEY = "scopaDataLoader.edge.times";

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
        int edgeTimes;
        private Counter badLineCount;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            graph = TitanFactory.open(conf.get(TITAN_CONF_KEY));
            label = conf.get(LABEL_KEY);
            key1Index = Integer.parseInt(conf.get(KEY1_INDEX_KEY));
            key2Index = Integer.parseInt(conf.get(KEY2_INDEX_KEY));
            timeIndex = Integer.parseInt(conf.get(TIME_INDEX_KEY));
            edgeTimes = Integer.parseInt(conf.get(EDGE_TIMES_KEY));

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
                long timeStamp = System.currentTimeMillis();
                try {
                    timeStamp = new SimpleDateFormat(timeFormat)
                            .parse(fields[timeIndex].split("\\.")[0]).getTime();
                } catch (ParseException e) {
                    logger.error("can't parse time string: " + fields[timeIndex]);
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

                byte[] lineBytes = Bytes.toBytes(line);
                for (int i = 0; i < edgeTimes; i++) {
                    byte[] row = Bytes.toBytes(edge.getId().toString() + '_' + (timeStamp + i));
                    Put put = new Put(row);
                    put.add(columnFamily, columnQualifier, lineBytes);
                    rowKey.set(row);
                    context.write(rowKey, put);
                    batchCnt++;
                }
                if (batchCnt >= 2000) {
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
        String titanLibDir = "/user/hadoop/titanLibsAll";  // TODO should be an argument
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
        logger.info("This is ScopaLoaderMR2 compiled after 11:30AM 2016/5/8");
        Thread.sleep(1000);

        if (args.length < 4) {
            System.out.println("Args: titanConf label indices(key1,key2,time) inputPath [edge_times]");
            System.exit(1);
        }
        String titanConf = args[0];
        String label = args[1];
        String indices = args[2];
        String inputPath = args[3];
        String edgeTimes = "100";
        if (args.length > 4) {
            Integer.parseInt(args[4]);
            edgeTimes = args[4];
        }

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
        if (!mgnt.containsPropertyKey("time"))
            mgnt.makePropertyKey("time").dataType(Long.class).make();
        if (!mgnt.containsEdgeLabel("rycc_relation"))
            mgnt.makeEdgeLabel("rycc_relation").make();
        mgnt.commit();
        graph.shutdown();
        // create hbase table;
        Configuration conf = new Configuration();
        String zookeeperQuorumKey = "hbase.zookeeper.quorum";
        String zookeeperQuorum = "k1222.mlamp.co:2181,k1223.mlamp.co:2181,k1230.mlamp.co:2181";
        if (!zookeeperQuorum.equals(conf.get(zookeeperQuorumKey))) {
            logger.error("Wrong " + zookeeperQuorumKey + ": " + conf.get(zookeeperQuorumKey));
            conf.set(zookeeperQuorumKey, zookeeperQuorum);
        }
        createTable(TABLE_NAME, conf);

        Job job = Job.getInstance(conf, "scopa titan edges loader " + edgeTimes + " times");

        job.setJarByClass(ScopaLoaderMR2.class);
        job.setMapperClass(WorkerMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //job.setNumReduceTasks(0);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(inputPath));
        NLineInputFormat.setNumLinesPerSplit(job, 4);
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
        job.getConfiguration().set(EDGE_TIMES_KEY, edgeTimes);

        // setup reducer by HFileOutputFormat2
        try (HTable table = new HTable(conf, TABLE_NAME)) {
            HFileOutputFormat2.configureIncrementalLoad(job, table);
        }

        job.waitForCompletion(true);

        logger.info("finished");
//        logger.info("finally, counting vertices and edges...");
//        graph = TitanFactory.open(titanConf);
//        long vertexCnt = 0, edgeCnt = 0;
//        for (Vertex v : graph.getVertices()) {
//            vertexCnt++;
//            edgeCnt += ((TitanVertex)v).getEdgeCount();
//        }
//        edgeCnt /= 2;
//        logger.info("vertex count = " + vertexCnt);
//        logger.info("edge count = " + edgeCnt);
//        graph.shutdown();
    }
}
