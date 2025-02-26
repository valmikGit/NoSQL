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
        docID = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Tokenize the line into words and track their positions.
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        int index = 0;
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!word.isEmpty()) {
                // Emit the document ID as key and a string "index,word" as value.
                context.write(new Text(docID), new Text(index + "," + word));
                index++;
            }
        }
    }
}
