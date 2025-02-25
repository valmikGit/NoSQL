package com.hadoop.wiki;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming each document is one input file with ID as the filename
        String docID = context.getConfiguration().get("map.input.file");

        // Tokenize words while tracking index positions
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        int index = 0;

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Normalize words
            if (!word.isEmpty()) {
                context.write(new Text(docID), new Text(index + "," + word));
                index++;
            }
        }
    }
}
