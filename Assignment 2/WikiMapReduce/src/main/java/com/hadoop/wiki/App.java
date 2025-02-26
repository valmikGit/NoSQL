package com.hadoop.wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WikiIndexer <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Wiki Indexer");

        // Print out configuration values for debugging
        System.out.println("fs.defaultFS = " + conf.get("fs.defaultFS"));
        System.out.println("mapreduce.framework.name = " + conf.get("mapreduce.framework.name"));
        System.out.println("yarn.resourcemanager.address = " + conf.get("yarn.resourcemanager.address"));

        job.setJarByClass(App.class);
        job.setMapperClass(WikiMapper.class);
        job.setReducerClass(WikiReducer.class);

        // Mapper outputs: key = Text, value = Text.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer outputs: key = IntWritable, value = Text.
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Use at least 3 reducers (change as needed)
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
