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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Iterator;

/**
 * Load vertices into Titan in parallel
 *
 * Created by huangql on Nov. 8, 2016.
 */
public class VertexLoaderMR {
    private static final Logger logger = Logger.getLogger(VertexLoaderMR.class);

    public static final String TITAN_CONF_KEY = "rawDataLoader.titan.conf";
    public static final String LABEL_KEY = "rawDataLoader.label";

    public static class VertexLoaderWorker
            extends Mapper<Object, Text, Text, IntWritable> {

        TitanGraph graph;
        String label;
        String col_delimiter = "\t";
        int currCount = 0;
        int keyIndex = 0;
        private Counter badLineCount, skippedLineCount;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            graph = TitanFactory.open(conf.get(TITAN_CONF_KEY));
            label = conf.get(LABEL_KEY);
            for (keyIndex = 0; !"key".equals(known_fields[keyIndex]); keyIndex++);
            badLineCount = context.getCounter("dataloader", "Bad Lines");
            skippedLineCount = context.getCounter("dataloader", "Skippd Lines");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String cols[] = value.toString().split(col_delimiter);
            if (cols.length <= 1) {
                badLineCount.increment(1);
                System.out.println("drop line: " + value);
                return;
            }

            String uniqueId = cols[keyIndex];
            Iterator<Vertex> vs = graph.query().has("key", uniqueId).limit(1).vertices().iterator();
            if (vs.hasNext()) {
                skippedLineCount.increment(1);
                System.out.println(label + " vertex with unique id " + uniqueId + " exists.");
                return;
            }

            TitanVertex v = graph.addVertexWithLabel(label);
            for (int i = 0; i < known_fields.length; i++) {
                if (!"NULL".equalsIgnoreCase(cols[i]))
                    v.addProperty(known_fields[i], cols[i]);
            }
            for (int i = known_fields.length; i < cols.length; i++) {
                if (!"NULL".equalsIgnoreCase(cols[i]))
                    v.addProperty("field" + i, cols[i]);
            }
            currCount++;
            if (currCount >= 10000) {
                graph.commit();
                currCount = 0;
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (graph != null) {
                graph.commit();
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

    // 34 known fields
    public static final String known_fields[] = ("bjdrybh\txm\twwxm\txb_dm\txb\tkey\tfake1\tcsrq\tgj_dm\tgj" +
            "\tmz_dm\tmz\tjg_dm\tjg\thjdqh\thjdxz\thjdpcsdm\thjdpcs\txzdqh\txzdxz" +
            "\txzdpcsdm\txzdpcs\tgxdwjgdm\tgxdwjg\tladwjgdm\tladwjg\tzjlasj\tnrbjzdryksj\tzdrylbbj_dm\tzdrylbbj" +
            "\tzdryxl_dm\tzdryxl\tyxx_dm\tyxx").split("\\t");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Args: titanConf inputPath [label]");
            System.exit(1);
        }
        String titanConf = args[0];
        String inputPath = args[1];
        String label = "person";
        if (args.length > 2) {
            label = args[2];
        }

        //
        // make vertex schema
        //
        TitanGraph graph = TitanFactory.open(titanConf);
        TitanManagement mgnt = graph.getManagementSystem();
        // vertex label
        if (!mgnt.containsVertexLabel(label))
            mgnt.makeVertexLabel(label).make();
        // unique index
        PropertyKey key;
        if (!mgnt.containsPropertyKey("key"))
            key = mgnt.makePropertyKey("key").dataType(String.class).make();
        else
            key = mgnt.getPropertyKey("key");
        if (!mgnt.containsGraphIndex("byKey")) {
            mgnt.buildIndex("byKey", Vertex.class).addKey(key).unique().buildCompositeIndex();
        }
        // other properties
        for (String field : known_fields) {
            if (!mgnt.containsPropertyKey(field))
                mgnt.makePropertyKey(field).dataType(String.class).make();
        }
        for (int i = known_fields.length; i < 50; i++) {
            if (!mgnt.containsPropertyKey("field"+i))
                mgnt.makePropertyKey("field"+i).dataType(String.class).make();
        }
        mgnt.commit();
        graph.shutdown();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HyBriG Vertex Loader");

        job.setJarByClass(VertexLoaderMR.class);
        job.setMapperClass(VertexLoaderWorker.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));
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
