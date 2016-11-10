package cn.edu.pku.hql.titan.mapreduce;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

/**
 * Count total number of edges adjacent to given vertex keys.
 *
 * Created by huangql on Nov. 10, 2016.
 */
public class EdgeCounterMR {
    private static final Logger logger = Logger.getLogger(EdgeCounterMR.class);

    public static final String TITAN_CONF_KEY = "rawDataLoader.titan.conf";
    public static final String CounterGroupName = "EdgeCounter";

    public static class VertexLoaderWorker
            extends Mapper<Object, Text, Text, IntWritable> {

        TitanGraph graph;
        int count = 0;
        private Counter edgeCounter;
        private Counter badKeyCounter;
        private Counter dupKeyCounter;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            graph = TitanFactory.open(conf.get(TITAN_CONF_KEY));
            edgeCounter = context.getCounter(CounterGroupName, "degree sum");
            badKeyCounter = context.getCounter(CounterGroupName, "bad keys");
            dupKeyCounter = context.getCounter(CounterGroupName, "duplicate keys");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Iterator<Vertex> it = graph.getVertices("key", value.toString()).iterator();
            if (!it.hasNext()) {
                badKeyCounter.increment(1);
                System.out.println("key " + value + " not found!");
                return;
            }
            count += ((TitanVertex)it.next()).getEdgeCount();
            if (it.hasNext()) {
                dupKeyCounter.increment(1);
                System.out.println("Duplicate key " + value);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (graph != null) {
                graph.commit();
                graph.shutdown();
            }
            edgeCounter.increment(count);
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Args: titanConf inputPath");
            System.exit(1);
        }
        String titanConf = args[0];
        String inputPath = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HyBriG Edge Counter");

        job.setJarByClass(EdgeCounterMR.class);
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

        job.waitForCompletion(true);

        System.out.println("Counter Results");
        for (CounterGroup g : job.getCounters()) {
            if (!CounterGroupName.equals(g.getName()))
                continue;
            for (Counter c : g) {
                System.out.println(c.getName() + "\t" + c.getValue());
            }
        }
    }
}
