package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class DocumentFrequency {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Creating Hadoop Job Configuration
        Job job = Job.getInstance(conf, "document frequency");

        // Adding cache files (stopwords and precomputed data)
        job.addCacheFile(new URI("/Users/mitta/Downloads/stopwords.txt"));
        job.addCacheFile(new URI("/Users/mitta/Downloads/part-r-00000"));

        job.setJarByClass(DocumentFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        // Setting input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
