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


//public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
    public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    int rowId;
    String body;
    String title;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString();


        //Extracts body
        Pattern pattern = Pattern.compile("Body=\"(.*?)\"");
        Matcher matcher = pattern.matcher(output);
        if (matcher.find()) {
            body = matcher.group(1);
        }
        //Extracts Title
        Pattern pattern2 = Pattern.compile("Title=\"(.*?)\"");
        Matcher matcher2 = pattern2.matcher(output);
        if (matcher2.find()) {
            title = matcher2.group(1);
        }
        //Extracts RowId
        Pattern pattern3 = Pattern.compile("row Id=\"(.*?)\"");
        Matcher matcher3 = pattern3.matcher(output);
        if (matcher3.find()) {
            rowId = Integer.parseInt(matcher3.group(1));
        }
        output = body + title;
        //System.out.println(output); For debug purposes
        output = output.replaceAll("\"", ""); //Removes the trailing tag
        output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
        output = output.replaceAll("\\<.*?>", ""); //Removes html tags
        output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
        output = output.replaceAll("\\d", " "); //Replaces digits with space
        output = output.replaceAll("_", " "); //Replaces underscore with space
        output = output.trim().replaceAll(" +", " "); //Removes any double spaces
        output = output.toLowerCase(); //Sets all characters to lower case

        final String[] words = output.split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(rowId));

        }


    }
}
