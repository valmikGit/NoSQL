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

            // Extract only the numeric part of the document ID
            String numericPart = parts[0].replaceAll("[^0-9]", ""); // Remove non-numeric characters

            // Ensure numericPart is not empty before parsing
            if (numericPart.isEmpty()) {
                continue; // Skip this entry if it has no valid timestamp
            }

            try {
                int timestamp = Integer.parseInt(numericPart); // Convert to integer
                String word = parts[1];

                // Select the word with the highest timestamp
                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                    latestWord = word;
                }
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid timestamp: " + parts[0]); // Log invalid values
            }
        }

        // Emit (index, latest word) if a valid word was found
        if (!latestWord.isEmpty()) {
            context.write(index, new Text(latestWord));
        }
    }

}
