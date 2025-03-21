package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeCoOccurrenceMatrix {
    public static class StripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private MapWritable stripe = new MapWritable();
        private Text word = new Text();
        private Set<String> frequentWords = new HashSet<>();
        private int distance = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                String filename = new Path(cacheFiles[0].getPath()).getName();
                loadFrequentWords(new File(filename));
            } else {
                System.err.println("No frequent words file found in cache!");
            }
            distance = context.getConfiguration().getInt("cooccurrence.distance", 1);
        }

        private void loadFrequentWords(File freqWordsFile) {
            try (BufferedReader reader = new BufferedReader(new FileReader(freqWordsFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.trim().toLowerCase().split("\\s+");
                    if (parts.length > 0 && !parts[0].isEmpty()) {
                        frequentWords.add(parts[0]);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading frequent words file: " + e.getMessage());
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(line, " .,;!?()[]{}<>:\"/\\|@#$%^&*+=~`_-");
            List<String> wordsList = new ArrayList<>();

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                wordsList.add(token);
            }

            int size = wordsList.size();
            for (int i = 0; i < size; i++) {
                String word1 = wordsList.get(i);
                if (!frequentWords.contains(word1)) continue;

                stripe.clear();
                for (int j = i + 1; j < size && j <= i + distance; j++) {
                    String word2 = wordsList.get(j);
                    if (!frequentWords.contains(word2)) continue;

                    Text neighbor = new Text(word2);
                    IntWritable count = (IntWritable) stripe.get(neighbor);
                    stripe.put(neighbor, new IntWritable(count == null ? 1 : count.get() + 1));
                }
                word.set(word1);
                context.write(word, stripe);
            }
        }
    }

    public static class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregatedStripe = new MapWritable();

            for (MapWritable stripe : values) {
                for (Writable neighbor : stripe.keySet()) {
                    IntWritable count = (IntWritable) stripe.get(neighbor);
                    IntWritable aggregatedCount = (IntWritable) aggregatedStripe.get(neighbor);
                    aggregatedStripe.put(neighbor, new IntWritable((aggregatedCount == null ? 0 : aggregatedCount.get()) + count.get()));
                }
            }
            context.write(key, new Text(aggregatedStripe.toString()));
        }
    }

    public static void main(String[] args) throws Exception{

            long startTime, endTime;

            if (args.length < 4) {
                System.err.println("Usage: StripeCoOccurrenceMatrix <input path> <output path> <frequent words file URI> <distance>");
                System.exit(-1);
            }

            int d = Integer.parseInt(args[3]); // Take d as input from the user

            startTime = System.currentTimeMillis();

            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setInt("cooccurrence.distance", d);

            Job job = Job.getInstance(conf, "Stripe Co-Occurrence Matrix d=" + d);
            job.setJarByClass(StripeCoOccurrenceMatrix.class);
            job.setMapperClass(StripeMapper.class);
            job.setReducerClass(StripeReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            job.addCacheFile(new URI(args[2])); // Add frequent words file

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1])); // Single output path

            job.setNumReduceTasks(1);

            boolean success = job.waitForCompletion(true);
            endTime = System.currentTimeMillis();
            System.out.println("Runtime for d=" + d + ": " + (endTime - startTime) + " ms");

            System.exit(success ? 0 : 1);


    }
}
