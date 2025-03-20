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

        String[] parts = line.split("\\s+", 2);

        if (parts.length < 2) return;

        try {

            int index = Integer.parseInt(parts[0]);

            String content = line.substring(line.indexOf('(') + 1, line.lastIndexOf(')'));

            String[] splits = content.split("\\s+", 2);
            if (splits.length < 2) return;

            String docID = splits[0].split("\\.")[0];

            long timestamp = Long.parseLong(docID)*100;
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String formattedTime = sdf.format(new Date(timestamp));

            String word = splits[1];

            context.write(new IntWritable(index), new Text(formattedTime + "," + word));

        } catch (NumberFormatException e) {

        }

    }
}
