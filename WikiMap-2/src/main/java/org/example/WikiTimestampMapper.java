package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiTimestampMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Read a line from the output of Problem 2
        String line = value.toString().trim();

        if (line.isEmpty()) return; // Skip empty lines

        // Split line into index and (doc-ID, word)
        String[] parts = line.split("\\s+", 2);
        if (parts.length < 2) return;

        int index = Integer.parseInt(parts[0]);
        String docWordPair = parts[1].replaceAll("[()]", ""); // Remove parentheses

        // Extract doc-ID and word
        String[] docWord = docWordPair.split(", ");
        if (docWord.length < 2) return;

        String docID = docWord[0];
        docID = docID.substring(0, docID.lastIndexOf('.'));

        long timestamp = Long.parseLong(docID);  // Convert to long

        // Convert to Instant and then to UTC time
        ZonedDateTime utcDateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);

        // Format the output
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        docID = utcDateTime.format(formatter);

        String word = docWord[1];

        // Emit (index, (timestamp, word))
        context.write(new IntWritable(index), new Text(docID + "," + word));
    }
}
