package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Should
        //final String line = value.toString();
        //final StringTokenizer tokenizer = new StringTokenizer(line);
        //while (tokenizer.hasMoreElements()) {
          //  word.set(tokenizer.nextToken());
//            context.write(word, UNO);
  //      }
        context.write(value, UNO);
    }
}
