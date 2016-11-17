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



public class WordCountReducer extends Reducer<ComKey,NullWritable,Text,DoubleWritable> {

    double counter = 0;

    protected void reduce(ComKey output, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

                counter++;
            }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new Text("Total number of questions with more than one answer: "), new DoubleWritable(counter));

    }


}





