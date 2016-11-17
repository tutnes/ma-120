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


public class WordCountMapper extends Mapper<LongWritable, Text, ComKey, NullWritable> {



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();
        if (output.contains("PostTypeId=\"1\"")) { //If it is a question
            int answerCount = 0;


            Pattern pattern2 = Pattern.compile("AnswerCount=\"(.*?)\""); //Extracts whats within reputation tag

            Matcher matcher2 = pattern2.matcher(output);

            if (matcher2.find()) {
                answerCount = Integer.parseInt(matcher2.group(1));
            }
            if (answerCount > 1) {
                context.write(new ComKey("samekey", answerCount), NullWritable.get());
            }
        }
    }



}

