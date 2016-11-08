package cn.edu.pku.hql.titan.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class SortEdge {

    public static class SortMapper
            extends Mapper<Object, Text, Text, Text> {
        Text sortKey = new Text();
        Text edgeValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            String key1 = fields[0];
            String key2 = fields[19];
            if (key1.compareTo(key2) < 0) {
                sortKey.set(key1 + key2);
                context.write(sortKey, value);
            } else {
                StringBuilder sb = new StringBuilder();
                for (int i = 19; i < fields.length; i++)
                    sb.append(fields[i]).append("\u0001");
                for (int i = 0; i < 18; i++)
                    sb.append(fields[i]).append("\u0001");
                sb.append(fields[18]);
                sortKey.set(key2 + key1);
                edgeValue.set(sb.toString());
                context.write(sortKey, edgeValue);
            }
        }
    }

    public static class SortReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: inputPath outputPath");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort edge");

        job.setJarByClass(SortEdge.class);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
