package cn.edu.pku.hql.titan.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by huangql on 5/3/16.
 */
public class DegreeDistribution {

    public static class EdgeToVertexMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        static Text vid = new Text();
        static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            String key1 = fields[0];
            String key2 = fields[19];
            vid.set(key1);
            context.write(vid, one);
            vid.set(key2);
            context.write(vid, one);
        }
    }

    public static class DegreeMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        static Text degree = new Text();
        static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            degree.set(fields[1]);
            context.write(degree, one);
        }
    }

    public static class CounterReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        static IntWritable count = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: edgeInputPath degreeOutputPath distributionOutputPath");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "analysis graph: degree distribution, step1");

        job.setJarByClass(DegreeDistribution.class);
        job.setMapperClass(EdgeToVertexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(CounterReducer.class);
        job.setReducerClass(CounterReducer.class);
        job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("job 1 failed");
            System.exit(1);
        }

        job = Job.getInstance(conf, "analysis graph: degree distribution, step2");
        job.setJarByClass(DegreeDistribution.class);
        job.setMapperClass(DegreeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(CounterReducer.class);
        job.setReducerClass(CounterReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
