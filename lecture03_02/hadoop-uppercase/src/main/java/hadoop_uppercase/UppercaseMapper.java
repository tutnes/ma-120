package hadoop_uppercase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class UppercaseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        Text word = new Text();
        String str = line.toString();
        str = str.toUpperCase();
        String[]d = str.split(" ");
        for (String z : d) {
            z = z.replaceAll("\\W", "");
            context.write(new Text(z), new IntWritable(1));
        }
    }
}




