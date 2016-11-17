package org.hadoop_seed.wordcount;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;

import java.io.IOException;
//import java.util.Map;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


//public class WordCountReducer extends Reducer<ComKey,IntWritable,ComKey,NullWritable> {
public class WordCountReducer extends Reducer<ComKey,IntWritable,ComKey,IntWritable> {
    int sum = 0;
    String userName;
    protected void reduce(ComKey output, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable value : values) {
            context.write(output, new IntWritable(1));
        }


    }

    //protected void cleanup(Context context) throws IOException, InterruptedException {

      //  context.write(new ComKey(userName, sum), NullWritable.get());


    //}

    //context.write(new IntWritable(maxCount), popularWord);
}





