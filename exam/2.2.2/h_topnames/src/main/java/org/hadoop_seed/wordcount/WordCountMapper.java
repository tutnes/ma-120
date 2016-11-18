package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, ComKey, IntWritable> {
    //private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().contains("DisplayName=")) { //If it is a question
            int reputation = 0;
            String output = value.toString(); //.replaceAll("ody=\"", ""); //Removes <></>he leading tag
            Pattern pattern = Pattern.compile("isplayName=\"(.*?)\"");



            Matcher matcher = pattern.matcher(output);

            if (matcher.find()) {
                output = matcher.group(1);
            }
            output = output.split(" ")[0];



        context.write(new ComKey(output, 1), new IntWritable(1));
        }

    }



}

