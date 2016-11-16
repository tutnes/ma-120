package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();
    //private TreeMap<Integer, Text> topusers = new TreeMap<Integer, Text>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().contains("DisplayName=")) { //If it is a question
            int reputation = 0;
            String output = value.toString(); //.replaceAll("ody=\"", ""); //Removes <></>he leading tag
            Pattern pattern = Pattern.compile("isplayName=\"(.*?)\"");
            Pattern pattern2 = Pattern.compile("Reputation=\"(.*?)\"");


            Matcher matcher = pattern.matcher(output);
            Matcher matcher2 = pattern2.matcher(output);
            if (matcher.find()) {
                output = matcher.group(1);
            }

            if (matcher2.find()) {
                reputation = Integer.parseInt(matcher2.group(1));
            }
        context.write(new Text(output), new IntWritable(reputation));
        }

    }

}

