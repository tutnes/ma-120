package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
        String output = value.toString(); //.replaceAll("ody=\"", ""); //Removes <></>he leading tag
        if (output.contains("DisplayName=")) { //If it is a question
            

            Pattern pattern = Pattern.compile("DisplayName=\"(.*?)\"");
    //            Pattern pattern = Pattern.compile("^(.*?)\"");

            Matcher matcher = pattern.matcher(output);
            if (matcher.find()) {
                output = matcher.group(1);
            }
            //System.out.println(output); For debug purposes
            /*output = output.replaceAll("\"", ""); //Removes the trailing tag
            output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
            output = output.replaceAll("\\<.*?>", ""); //Removes html tags
            output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            output = output.replaceAll("\\d", " "); //Replaces digits with space
            output = output.replaceAll("_", " "); //Replaces underscore with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
            output = output.toLowerCase(); //Sets all characters to lower case*/
            //Iterates through the ouputstring, splits it in words
            final String word = output.split(" ")[0];
            //for (String word : words) {
                context.write(new Text(word), UNO);

            //}
        }

    }
}
