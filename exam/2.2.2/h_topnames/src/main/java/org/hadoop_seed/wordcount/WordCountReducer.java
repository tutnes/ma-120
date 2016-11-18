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
    String userName;
    int counter = 0;
    int sum = 0;
    //ArrayList
    //TreeMap tmp = new TreeMap();
    protected void reduce(Iterable<ComKey> output, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //for (IntWritable value : values) {
        for (ComKey value : output) {
            //tmp.put()
            sum = sum + value.getreputation();
            //context.write(new ComKey(value.getuserName(),value.getreputation()), new IntWritable(value.getreputation()));
            //context.write(new ComKey(output.getuserName(),1), new IntWritable(1));
            System.out.println(value.getuserName() + value.getreputation());        //counter++;
            }
        //userName = output.getuserName();

        }



    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new ComKey(userName,sum), new IntWritable(sum));


    }
}

    //context.write(new IntWritable(maxCount), popularWord);






