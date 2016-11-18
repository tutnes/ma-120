package org.hadoop_seed.wordcount;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();
        if (output.contains("PostTypeId=\"1\"")) { //If it is a question
            Pattern pattern = Pattern.compile("ody=\"(.*?)\"");
            Matcher matcher = pattern.matcher(output);

            if (matcher.find()) {
                output = matcher.group(1);
              //  System.out.println(output);
                if (output.toLowerCase().contains("if")) {
                    context.write(new Text("useless"), new IntWritable(1));
                }
            }



            }
        }
    }





