package org.hadoop_seed;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.hadoop_seed.wordcount.WordCountMapper;
import org.hadoop_seed.wordcount.WordCountReducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class WordCountTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new WordCountMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new WordCountReducer());
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("lorem ipsum dolor ipsum"));
        mapDriver.withOutput(new Text("lorem"), new IntWritable(1));
        mapDriver.withOutput(new Text("ipsum"), new IntWritable(1));
        mapDriver.withOutput(new Text("dolor"), new IntWritable(1));
        mapDriver.withOutput(new Text("ipsum"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        final List<IntWritable> values = new ArrayList<IntWritable>(asList(new IntWritable(1), new IntWritable(1)));
        reduceDriver.withInput(new Text("lorem"), values);
        reduceDriver.withOutput(new Text("lorem"), new IntWritable(2));
        reduceDriver.runTest();
    }
}
