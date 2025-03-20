import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CooccurrencePairsDriverWithLocalAggregationEnabled {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Set distance dynamically (pass it as argument)
        if (args.length > 2) {
            conf.setInt("distance", Integer.parseInt(args[2]));
        } else {
            conf.setInt("distance", 1); // Default distance
        }

        Job job = Job.getInstance(conf, "co-occurrence pairs with local aggregation");
        job.setJarByClass(CooccurrencePairsDriver.class);
        
        // Set Mapper and Reducer classes
        job.setMapperClass(CooccurrencePairsMapper.class);
        job.setReducerClass(CooccurrencePairsReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
