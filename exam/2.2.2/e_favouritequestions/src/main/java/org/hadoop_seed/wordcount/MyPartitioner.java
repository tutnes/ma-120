package org.hadoop_seed.wordcount;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends
        Partitioner<ComKey, NullWritable> {
    @Override
    public int getPartition(ComKey key, NullWritable value,
                            int numReduceTasks) {
        return (key.getuserName().hashCode() % numReduceTasks);
    }
}
