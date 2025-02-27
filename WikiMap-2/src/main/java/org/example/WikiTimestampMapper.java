package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class WikiTimestampMapper extends Mapper<Object, Text, IntWritable, Text> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return; // Skip empty lines

        // Split line into index and (doc-ID, word)
        String[] parts = line.split("\\s+", 2);
        if (parts.length < 2) return;

        int index = Integer.parseInt(parts[0]);
        String docWordPair = parts[1].replaceAll("[()]", ""); // Remove parentheses

        // Extract doc-ID and word
        String[] docWord = docWordPair.split(",");
        if (docWord.length < 2) return;

        String docID = docWord[0].trim();
        String word = docWord[1].trim();

        // Extract timestamp from docID (before any dot if present)
        int dotIndex = docID.lastIndexOf('.');
        if (dotIndex != -1) {
            docID = docID.substring(0, dotIndex);
        }

        // Convert timestamp to long
        long timestamp;
        try {
            timestamp = Long.parseLong(docID);
        } catch (NumberFormatException e) {
            System.err.println("Invalid timestamp: " + docID);
            return;
        }

        // Convert timestamp to formatted UTC date
        ZonedDateTime utcDateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);
        String formattedDate = utcDateTime.format(formatter);

        // Emit (index, "formattedDate,word")
        System.out.println("Mapper Output: " + index + " " + formattedDate + " " + word);
        context.write(new IntWritable(index), new Text(formattedDate + "," + word));
    }
}
