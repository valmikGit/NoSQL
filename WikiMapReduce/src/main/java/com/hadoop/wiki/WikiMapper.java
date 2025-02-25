package com.hadoop.wiki;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

import java.io.IOException; /**
 * Hello world!
 *
 */

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2); // Assuming tab-separated DocID and content
            if (parts.length < 2) return;

            String docID = parts[0];
            String content = parts[1];

            StringTokenizer tokenizer = new StringTokenizer(content);
            int index = 0;

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                context.write(new Text(docID), new Text(index + "," + word));
                index++;
            }
        }
}
