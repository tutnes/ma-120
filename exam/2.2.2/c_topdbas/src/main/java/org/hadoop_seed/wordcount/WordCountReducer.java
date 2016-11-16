package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class WordCountReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
    private TreeMap<IntWritable, Text> topusers = new TreeMap<IntWritable, Text>();

    Text popularWord = new Text();
    int maxCount = 0;
    protected void reduce(Text name, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }

        //if (sum > maxCount) {
            maxCount = sum;
            popularWord = name;
        //}

        //context.write(popularWord, new IntWritable(maxCount));
        context.write(new IntWritable(maxCount), popularWord);
        //System.out.println(name.toString());
        //topusers.put(new IntWritable(sum),name);
        //if (topusers.size() > 10) {
        //    topusers.remove(topusers.lastKey());
        //}
    }

/*    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<IntWritable, Text> entry : topusers.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue().toString());
        }
        //context.write(popularWord, new IntWritable(maxCount));
        context.write(new IntWritable(maxCount), popularWord);
    }*/

}


