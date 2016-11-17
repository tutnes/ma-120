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
    //private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();
    //private TreeMap<Integer, Text> topusers = new TreeMap<Integer, Text>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();
        if (output.contains("PostTypeId=\"1\"")) { //If it is a question
            int reputation = 0;

            //Pattern pattern = Pattern.compile("ody=\"(.*?)\""); //Extracts whats within Body tag
            Pattern pattern2 = Pattern.compile("AnswerCount=\"(.*?)\""); //Extracts whats within reputation tag
         //   Matcher matcher = pattern.matcher(output);
            Matcher matcher2 = pattern2.matcher(output);
            //if (matcher.find()) {
//                output = matcher.group(1);
  //          }

            if (matcher2.find()) {
                reputation = Integer.parseInt(matcher2.group(1));
            }



      /*      output = output.replaceAll("\"", ""); //Removes the trailing tag
            output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
            output = output.replaceAll("\\<.*?>", ""); //Removes html tags
            //output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            //output = output.replaceAll("\\d", " "); //Replaces digits with space
            //output = output.replaceAll("_", " "); //Replaces underscore with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
*/




        context.write(new ComKey("samekey", reputation), NullWritable.get());
        }

    }



}

