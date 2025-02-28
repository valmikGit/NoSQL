
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
    private String latestTimestamp = "00:00:00";
    private List<IntWritable> indexList = new ArrayList<>();
    private List<String> wordList = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] data = value.toString().split(",", 2);
            if (data.length < 2)
                continue;

            String currentTimestamp = data[0].trim();
            String currentWord = data[1].trim();

            // Update lists if a new latest timestamp is found
            if (currentTimestamp.compareTo(latestTimestamp) > 0) {
                latestTimestamp = currentTimestamp;
                wordList.clear();
                indexList.clear();
                indexList.add(new IntWritable(key.get()));
                wordList.add(currentWord);
            }
            // Add words and indexes sharing the latest timestamp
            else if (currentTimestamp.equals(latestTimestamp)) {
                indexList.add(new IntWritable(key.get()));
                wordList.add(currentWord);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit all words and indexes with the latest timestamp
        for (int i = 0; i < indexList.size(); i++) {
            context.write(indexList.get(i), new Text(wordList.get(i) + ", " + latestTimestamp));
        }
    }
}
