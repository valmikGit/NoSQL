import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

public class PairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Set<String> stopWords = new HashSet<>();
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load stop words from distributed cache
        BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            stopWords.add(line.trim().toLowerCase());
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", "");
        String[] tokens = line.split("\\s+");

        for (String token : tokens) {
            if (!stopWords.contains(token) && token.length() > 0) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}