package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString().replaceAll("ody=\"",""); //Removes the leading tag
        output = output.replaceAll("\"",""); //Removes the trailing tag
        output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
        output = output.replaceAll("\\<.*?>",""); //Removes html tags
        output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
        output = output.replaceAll("\\d", " "); //Replaces digits with space
        output = output.replaceAll("_", " "); //Replaces underscore with space
        output = output.trim().replaceAll(" +", " "); //Removes any double spaces

        //Iterates through the ouputstring, splits it in words
        final String[] words = output.split(" ");
        for (String word : words) {

                context.write(new Text(word), UNO);

        }
    }
}
