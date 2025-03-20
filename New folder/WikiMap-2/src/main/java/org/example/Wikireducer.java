
package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Wikireducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxTime = "00:00:00";
        String latestWord = "";
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length < 2) continue;

            String timestamp = parts[0].trim();
            String word = parts[1].trim();

            if (timestamp.compareTo(maxTime) > 0) {
                maxTime = timestamp;
                latestWord = word;
            }
        }
        // Emit (index, latest word) if a valid word was found
        if (!latestWord.isEmpty()) {
            System.out.println("Reducer Output: " + key + " " + latestWord);
            context.write(key, new Text(latestWord+", "+maxTime));
        }
    }
}
