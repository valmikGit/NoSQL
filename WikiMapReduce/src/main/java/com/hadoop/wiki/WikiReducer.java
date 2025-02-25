package com.hadoop.wiki;



import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WikiReducer extends Reducer<Text, Text, IntWritable, Text> {

    @Override
    protected void reduce(Text docID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> wordList = new ArrayList<>();

        // Collect and sort words by index
        for (Text val : values) {
            wordList.add(val.toString());
        }

        Collections.sort(wordList, (a, b) -> {
            int indexA = Integer.parseInt(a.split(",")[0]);
            int indexB = Integer.parseInt(b.split(",")[0]);
            return Integer.compare(indexA, indexB);
        });

        // Emit in (index, (doc-ID, word)) format
        for (String entry : wordList) {
            String[] parts = entry.split(",");
            int index = Integer.parseInt(parts[0]);
            String word = parts[1];

            context.write(new IntWritable(index), new Text("(" + docID.toString() + ", " + word + ")"));
        }
    }
}

