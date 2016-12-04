package cn.edu.pku.hql.titan.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Sort Edges of RYCC and remove duplicated.
 */
public class UniqEdge {

    public static class UniqMapper
            extends Mapper<Object, Text, NullWritable, Text> {
        private static final String FIELD_DELIMITER = "\u0001";
        enum Counter {
            duplicated
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            String currKey1 = null, currKey2 = null;
            Set<String> timeSet = new HashSet<>();
            while (context.nextKeyValue()) {
                Text line = context.getCurrentValue();
                String[] fields = line.toString().split(FIELD_DELIMITER);
                String key1 = fields[0];
                String key2 = fields[19];
                String time = fields[9];
                if (!key1.equals(currKey1) || !key2.equals(currKey2)) {
                    currKey1 = key1;
                    currKey2 = key2;
                    timeSet.clear();
                }
                if (timeSet.contains(time)) {
                    context.getCounter(Counter.duplicated).increment(1);
                } else {
                    timeSet.add(time);
                    context.write(NullWritable.get(), line);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: inputPath outputPath");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unique sorted edge");

        job.setJarByClass(UniqEdge.class);
        job.setMapperClass(UniqMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
