import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CooccurencePairsMapperWithLocalAggregation extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> localAggregation;
    private int distance;
    private final static IntWritable one = new IntWritable(1);
    private Text pair = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize in-memory hashmap for local aggregation
        localAggregation = new HashMap<>();
        distance = context.getConfiguration().getInt("distance", 1); // Default distance = 1
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", ""); // Clean text
        String[] tokens = line.split("\\s+");

        for (int i = 0; i < tokens.length; i++) {
            for (int j = i + 1; j < Math.min(i + distance + 1, tokens.length); j++) {
                if (!tokens[i].equals(tokens[j])) {
                    String wordPair = tokens[i] + "," + tokens[j];
                    localAggregation.put(wordPair, localAggregation.getOrDefault(wordPair, 0) + 1);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit all locally aggregated pairs at the end of the Mapper
        for (Map.Entry<String, Integer> entry : localAggregation.entrySet()) {
            pair.set(entry.getKey());
            context.write(pair, new IntWritable(entry.getValue()));
        }
    }
}