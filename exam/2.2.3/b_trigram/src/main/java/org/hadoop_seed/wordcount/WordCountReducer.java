package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
/*    Text bigram = new Text();
    int highestCount = 0;
    @Override
    protected void reduce(Text inputString, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (final IntWritable tempCount : values) {
            count+=tempCount.get();

        }
        //context.write(key, new IntWritable(count));
        if (count > highestCount) {

            bigram = inputString;
            highestCount = count;
            System.out.println("Bigram: " + bigram + " highest count: " +  highestCount);
            //context.write(bigram, new IntWritable(highestCount));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(bigram, new IntWritable(highestCount));
    }
}
*/


    Text popularWord = new Text();
    int maxCount = 0;

    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
        if (sum > maxCount) {
            maxCount = sum;
            popularWord = word;
            context.write(popularWord, new IntWritable(maxCount));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      //  context.write(popularWord, new IntWritable(maxCount));
    }
}