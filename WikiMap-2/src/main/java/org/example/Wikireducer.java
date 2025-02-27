package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class Wikireducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text index, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String latestWord = "";
        LocalDate maxDate = LocalDate.MIN; // Start with the smallest possible date

        for (Text val : values) {
            String[] parts = val.toString().split(",");

            // Ensure valid input format (date, word)
            if (parts.length < 2) continue;

            String dateStr = parts[0].trim(); // Extract date part
            String word = parts[1].trim(); // Extract word part

            try {
                LocalDate date = LocalDate.parse(dateStr); // Convert string to LocalDate

                // Update latest word if a newer date is found
                if (date.isAfter(maxDate)) {
                    maxDate = date;
                    latestWord = word;
                }
            } catch (DateTimeParseException e) {
                System.err.println("Skipping invalid date: " + dateStr);
            }
        }

        // Emit (index, latest word) if a valid word was found
        if (!latestWord.isEmpty()) {
            context.write(index, new Text(latestWord));
        }
    }
}
