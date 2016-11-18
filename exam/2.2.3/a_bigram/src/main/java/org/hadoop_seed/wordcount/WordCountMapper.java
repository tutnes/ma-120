package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable UNO = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String output = value.toString(); //.replaceAll("ody=\"", ""); //Removes <></>he leading tag
        if (output.contains("PostTypeId=\"1\"")) { //If it is a question


            Pattern pattern = Pattern.compile("Title=\"(.*?)\"");
            Matcher matcher = pattern.matcher(output);
            if (matcher.find()) {
                output = matcher.group(1);
            }

            output = output.replaceAll("\"", ""); //Removes the trailing tag
            output = StringEscapeUtils.unescapeHtml(output); //Unescapes the html tags
            output = output.replaceAll("\\<.*?>", ""); //Removes html tags
            output = output.replaceAll("\\W", " "); //Replaces non alphanumeric characters with space
            output = output.replaceAll("\\d", " "); //Replaces digits with space
            output = output.replaceAll("_", " "); //Replaces underscore with space
            output = output.trim().replaceAll(" +", " "); //Removes any double spaces
            output = output.toLowerCase(); //Sets all characters to lower case
            //Iterates through the ouputstring, splits it in words
            NgramIterator bigram = new NgramIterator(2, output);
            while (bigram.hasNext()) {
                context.write(new Text(bigram.next()), UNO);
            }
        }
    }

    public   class NgramIterator implements Iterator<String> {

            String[] words;
            int pos = 0, n;

            public NgramIterator(int n, String str) {
                this.n = n;
                words = str.split(" ");
            }

            public boolean hasNext() {
                return pos < words.length - n + 1;
            }

            public String next() {
                StringBuilder sb = new StringBuilder();
                for (int i = pos; i < pos + n; i++)
                    sb.append((i > pos ? " " : "") + words[i]);
                pos++;
                return sb.toString();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        }



}

