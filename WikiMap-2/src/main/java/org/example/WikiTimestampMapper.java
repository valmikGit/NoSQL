package org.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class WikiTimestampMapper extends Mapper<Object, Text, IntWritable, Text> {
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
        // Convert timestamp to formatted UTC date
//        ZonedDateTime utcDateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);
//        String formattedDate = utcDateTime.format(formatter);
        long timestamp = Long.parseLong(docID)*10;
        // If you did not want to convert it into timestamp then comment down below 3 lines
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String formattedTime = sdf.format(new Date(timestamp));

        // Emit (index, "formattedDate,word")
        System.out.println("Mapper Output: " + index + " " + formattedTime + " " + word);
        context.write(new IntWritable(index), new Text(formattedTime + "," + word));
    }
}
