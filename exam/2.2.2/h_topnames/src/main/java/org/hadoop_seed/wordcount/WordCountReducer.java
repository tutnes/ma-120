package org.hadoop_seed.wordcount;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;

import java.io.IOException;
//import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;



public class WordCountReducer extends Reducer<ComKey,IntWritable,ComKey,IntWritable> {
    ComKey userName = new ComKey();
    int counter = 0;
    int sum = 0;

    //ArrayList
    //TreeMap tmp = new TreeMap();
    //protected void reduce(ComKey output, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    protected void reduce(ComKey output, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        userName = output;
        for (IntWritable value : values) {
            //for (ComKey value : output) {
            //tmp.put()
            sum = sum + value.get();
            //context.write(new ComKey(value.getuserName(),value.getreputation()), new IntWritable(value.getreputation()));
            //context.write(new ComKey(output.getuserName(),1), new IntWritable(1));


            //System.out.println(output.getuserName()+ " " + value.get());        //counter++;
        }


    }
    //context.write(new ComKey(userName,sum), new IntWritable(sum));


    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(userName, new IntWritable(sum));


    }
}


    //context.write(new IntWritable(maxCount), popularWord);






