package hadoop_uppercase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        int ret = 0;
        String s = text.toString();
        if (s.length() > 0) {
            if ("AEIOUaeiou".indexOf(s.charAt(0)) != -1) {
                ret = 1;
            } else {
                ret = 0;
            }
        }
        return ret;
    }
}
