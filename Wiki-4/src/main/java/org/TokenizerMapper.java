package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private Set<String> stopWords = new HashSet<>();
    private PorterStemmer stemmer = new PorterStemmer();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load stopwords from file
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim());
                }
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        MapWritable tfMap = new MapWritable();

        if (line != null) {
            SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
            String[] tokenizedLine = tokenizer.tokenize(line); // Tokenize line

            for (String s : tokenizedLine) {
                if (!stopWords.contains(s)) {
                    String word = stemmer.stem(s.toLowerCase());
                    Text wordText = new Text(word);

                    if (tfMap.containsKey(wordText)) {
                        IntWritable temp = (IntWritable) tfMap.get(wordText);
                        temp.set(temp.get() + 1);
                    } else {
                        tfMap.put(wordText, new IntWritable(1));
                    }
                }
            }
            context.write(new Text(fileName), tfMap);
        }
    }
}
