package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<ComKey,IntWritable,ComKey,NullWritable> {

        protected void reduce(ComKey out, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

            context.write(out, NullWritable.get());
        }
    }

/*    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<IntWritable, Text> entry : topusers.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue().toString());
        }
        //context.write(popularWord, new IntWritable(maxCount));
        context.write(new IntWritable(maxCount), popularWord);
    }*/

}


