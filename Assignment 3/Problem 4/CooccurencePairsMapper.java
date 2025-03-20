import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class CooccurencePairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text pair = new Text();
    private int distance;

    @Override
    protected void setup(Context context) {
        distance = context.getConfiguration().getInt("distance", 1); // Default distance 1
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", "");
        String[] tokens = line.split("\\s+");

        for (int i = 0; i < tokens.length; i++) {
            for (int j = i + 1; j < Math.min(i + distance + 1, tokens.length); j++) {
                if (!tokens[i].equals(tokens[j])) {
                    pair.set(tokens[i] + "," + tokens[j]);
                    context.write(pair, one);
                }
            }
        }
    }
}