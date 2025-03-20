import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class CooccurenceStripeReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) 
            throws IOException, InterruptedException {
        MapWritable stripe = new MapWritable();

        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                Text neighbor = (Text) entry.getKey();
                IntWritable count = (IntWritable) entry.getValue();

                if (stripe.containsKey(neighbor)) {
                    IntWritable sum = (IntWritable) stripe.get(neighbor);
                    sum.set(sum.get() + count.get());
                } else {
                    stripe.put(neighbor, new IntWritable(count.get()));
                }
            }
        }
        context.write(key, stripe);
    }
}