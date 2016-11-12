package cn.edu.pku.hql.titan.mapreduce;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

    public static class Map extends TableMapper<NullWritable, NullWritable> {

        private StandardTitanGraph graph;
        private IDManager idManager = new IDManager();
        private int vertexCount = 0;
        private int edgeCount = 0;

        public void setup(Context context) throws IOException, InterruptedException {
            graph = (StandardTitanGraph) TitanFactory.open(
                    context.getConfiguration().get(TITAN_CONF_KEY));
            //graph.getEdgeSerializer().readRelation(entry, false, tx);
        }

        public void map(ImmutableBytesWritable key, Result value,
                        Context context) throws IOException, InterruptedException {
            long vid = idManager.getKeyID(new StaticArrayBuffer(key.get()));
            if (!IDManager.VertexIDType.NormalVertex.is(vid))
                return;

            vertexCount++;
            value.getFamilyMap(Backend.EDGESTORE_NAME.getBytes());
            new StaticBufferEntry()
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
        }
        String snapshotName = args[0];
        String titanConf = args[1];

        Job job = Job.getInstance(getConf(), "Titan vertices & edges counter");
        job.setJarByClass(SnapshotCounter.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TableSnapshotInputFormat.class);
        TableMapReduceUtil.initTableSnapshotMapperJob(
                snapshotName,
                new Scan().addFamily(Backend.EDGESTORE_NAME.getBytes()),
                SnapshotCounter.Map.class,
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
        ToolRunner.run(new Configuration(), new SnapshotCounter(), args);
    }
}
