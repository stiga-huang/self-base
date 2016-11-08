package cn.edu.pku.hql.titan.mapreduce;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * Created by huangql on 4/22/16.
 */
public class RawLoaderMR2 {
    private static final Logger logger = Logger.getLogger(RawLoaderMR2.class);

    public static final String TITAN_CONF_KEY = "rawDataLoader.titan.conf";
    public static final String LABEL_KEY = "rawDataLoader.label";
    public static final String KEY1_INDEX_KEY = "rawDataLoader.key1.index";
    public static final String KEY2_INDEX_KEY = "rawDataLoader.key2.index";
    public static final String TIME_INDEX_KEY = "rawDataLoader.time.index";
    public static final String EDGE_TIMES_KEY = "rawDataLoader.edge.times";

    public static class WorkerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        static final String timeFormat = "yyyy-MM-dd HH:mm:ss";

        TitanGraph graph;
        String label;
        int key1Index, key2Index, timeIndex;
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
                long timeStamp = System.currentTimeMillis();
                try {
                    timeStamp = new SimpleDateFormat(timeFormat)
                            .parse(fields[timeIndex].split("\\.")[0]).getTime();
                } catch (ParseException e) {
                    logger.error("can't parse time string: " + fields[timeIndex]);
                }

                for (int i = 0; i < edgeTimes; i++) {
                    TitanEdge edge = v1.addEdge(label, v2);
                    edge.setProperty("time", timeStamp + i * 1000);
                    edge.setProperty("value", line);
                    batchCnt++;
                }

                if (batchCnt >= 20000) {
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

    public static void main(String[] args) throws Exception {
        logger.info("This is RawLoaderMR2 compiled after 11:30AM 2016/5/8");
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

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "raw titan edges loader " + edgeTimes + " times");

        job.setJarByClass(RawLoaderMR2.class);
        job.setMapperClass(WorkerMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(inputPath));
        NLineInputFormat.setNumLinesPerSplit(job, 4);
        job.setOutputFormatClass(NullOutputFormat.class);

        // make sure every worker running uniquely
        job.setSpeculativeExecution(false);
        // default task timeout is 10min, set it to 0 to disable timeout
        job.getConfiguration().set("mapreduce.task.timeout", "0");

        setupClassPath(job);

        // upload titanConf and add to distributed cache
        File file = new File(titanConf);
        if (!file.exists())
            throw new FileNotFoundException(titanConf);
        String baseName = file.getName();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(file.toURI());
        Path dst = new Path("/tmp/RawLoaderMR/" + baseName);
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
