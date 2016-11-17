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
public class WordCountReducer extends Reducer<ComKey,NullWritable,ComKey,NullWritable> {

    int counter = 0;
    protected void reduce(ComKey output, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {
            //if (counter < 10) {

                counter++;
            }
        context.write(new ComKey(output.getuserName(),counter), NullWritable.get());
        }
    }

/*    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (hello : top10map) {
            System.out.println("Key: " + hello. + ". Value: " + output.getreputation());
        }

    }*/

    //context.write(new IntWritable(maxCount), popularWord);






