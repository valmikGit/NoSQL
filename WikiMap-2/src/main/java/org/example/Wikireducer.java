package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class Wikireducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        LocalDateTime maxDateTime = null;
        String latestWord = "";

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length < 2) continue;

            String dateStr = parts[0].trim();
            String word = parts[1].trim();

            try {
                LocalDateTime dateTime = LocalDateTime.parse(dateStr, formatter);

                // If this date is later than the current maxDateTime, update
                if (maxDateTime == null || dateTime.isAfter(maxDateTime)) {
                    maxDateTime = dateTime;
                    latestWord = word;
                }
            } catch (DateTimeParseException e) {
                System.err.println("Skipping invalid date: " + dateStr);
            }
        }

        // Emit (index, latest word) if a valid word was found
        if (!latestWord.isEmpty()) {
            System.out.println("Reducer Output: " + key + " " + latestWord);
            context.write(key, new Text(latestWord));
        }
    }
}


