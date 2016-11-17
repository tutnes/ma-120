package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, ComKey, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();
        if (output.contains("DisplayName=")) { //If it contains a username
            Pattern pattern = Pattern.compile("isplayName=\"(.*?)\"");
            Matcher matcher = pattern.matcher(output);

            if (matcher.find()) {
                output = matcher.group(1);
            }
            output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
            final String[] words = output.split(" "); //Splits by space
            String firstName;
            if (words.length < 0) {
                firstName = output;//Arrays.asList(words).get(0);
            }
            else {
                firstName = Arrays.asList(words).get(0);
            }
            System.out.println(output);
            context.write(new ComKey(output, 1), UNO);
        }

    }



}

