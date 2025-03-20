import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class CooccurrenceStripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private Text word = new Text();
    private int distance;

    @Override
    protected void setup(Context context) {
        distance = context.getConfiguration().getInt("distance", 1);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", "");
        String[] tokens = line.split("\\s+");

        for (int i = 0; i < tokens.length; i++) {
            MapWritable stripe = new MapWritable();
            for (int j = i + 1; j < Math.min(i + distance + 1, tokens.length); j++) {
                if (!tokens[i].equals(tokens[j])) {
                    Text neighbor = new Text(tokens[j]);
                    if (stripe.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) stripe.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighbor, new IntWritable(1));
                    }
                }
            }
            word.set(tokens[i]);
            context.write(word, stripe);
        }
    }
}