package org.hadoop_seed.wordcount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends
        Partitioner<ComKey, IntWritable> {
    @Override
    public int getPartition(ComKey key, IntWritable value,
                            int numReduceTasks) {
        return (key.getuserName().hashCode() % numReduceTasks);
    }
}
