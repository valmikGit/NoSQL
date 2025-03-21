package org;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class IntSumReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
    private Map<String, Integer> dfMap = new HashMap<>();
    private LinkedHashMap<String, Integer> finalDf = new LinkedHashMap<>();
    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load document frequency data
        URI[] files = context.getCacheFiles();
        Path dfPath = new Path(files[1].getPath());

        try (BufferedReader reader = new BufferedReader(new FileReader(dfPath.toString()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String term = parts[0];
                int df = Integer.parseInt(parts[1]);
                dfMap.put(term, df);
            }

            // Sort document frequencies in ascending order
            List<Map.Entry<String, Integer>> dfList = new LinkedList<>(dfMap.entrySet());
            dfList.sort(Map.Entry.comparingByValue());

            int i = 0;
            for (Map.Entry<String, Integer> entry : dfList) {
                if (i < 100) {
                    finalDf.put(entry.getKey(), entry.getValue());
                    i++;
                } else break;
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable termFrequencyMap = new MapWritable();

        // Merge term frequency maps from different mappers
        for (MapWritable value : values) {
            for (MapWritable.Entry<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable> e : value.entrySet()) {
                Text term = (Text) e.getKey();
                IntWritable tf = (IntWritable) e.getValue();

                if (termFrequencyMap.containsKey(term)) {
                    IntWritable currentCount = (IntWritable) termFrequencyMap.get(term);
                    termFrequencyMap.put(term, new IntWritable(currentCount.get() + tf.get()));
                } else {
                    termFrequencyMap.put(term, tf);
                }
            }
        }

        // Compute TF-IDF scores
        for (MapWritable.Entry<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable> entry : termFrequencyMap.entrySet()) {
            String term = entry.getKey().toString();
            int tf = ((IntWritable) entry.getValue()).get();
            int df = finalDf.getOrDefault(term, 0);

            if (df == 0) continue; // Avoid division by zero
            double score = tf * Math.log10(10000.0 / (df + 1));
            result.set(score);
            context.write(new Text(key + "\t" + term), result);
        }
    }
}
