package org.hadoop_seed.wordcount;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;

import java.io.IOException;
//import java.util.Map;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;



public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    int counter = 0;

    protected void reduce(Text input, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable value : values) {
                counter = value.get() + counter;
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Question bodies with useless in them : " +  counter);
        context.write(new Text("Question bodies with useless in them : "), new IntWritable(counter));

    }


}





