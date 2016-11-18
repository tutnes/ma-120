package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,NullWritable> {



    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        List<Integer> myList = new ArrayList<Integer>();

        for (IntWritable val : values) {
            if (!myList.contains(val.get())) {
                myList.add(val.get());
            }

        }





        String output = key.toString() + myList.toString();
        context.write(new Text(output),NullWritable.get());
    }
    }




