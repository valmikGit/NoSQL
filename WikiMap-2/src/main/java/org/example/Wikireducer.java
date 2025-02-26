package org.example;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Wikireducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable index, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String latestWord = "";
        int maxTimestamp = Integer.MIN_VALUE;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length < 2) continue;

            int timestamp = Integer.parseInt(parts[0]); // Extract timestamp (doc-ID)
            String word = parts[1];

            // Select the word with the highest timestamp
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                latestWord = word;
            }
        }

        // Emit (index, latest word)
        context.write(index, new Text(latestWord));
    }
}
