package com.hadoop.wiki;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.StringTokenizer;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String docID;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get the file name from the input split as the document ID.
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        if (fileSplit != null && fileSplit.getPath() != null) {
            docID = fileSplit.getPath().getName();
        } else {
            docID = "unknown_doc";  // Fallback to prevent NullPointerException
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null || value.toString().trim().isEmpty()) {
            return; // Skip empty lines
        }

        String line = value.toString().trim();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int index = 0;

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();

            if (word != null && !word.isEmpty()) {
                context.write(new Text(docID), new Text(index + "," + word));
                index++;
            }
        }
    }
}
