import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Add stopwords to distributed cache
        Job job = Job.getInstance(conf, "word count with pairs");
        job.addCacheFile(new Path(args[2]).toUri()); // args[2] = path to stopwords.txt

        job.setJarByClass(PairsDriver.class);
        job.setMapperClass(PairsMapper.class);
        job.setReducerClass(PairsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}