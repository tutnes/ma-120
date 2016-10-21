import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class HadoopCustomReader {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(HadoopCustomReader.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println("value:\n" + value);
            //System.out.println("=======================");
            //final String[] words = value.toString().split("\\s+");
            //for (String word : words) {
            String studentXML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"+value.toString().trim();
            try {
                Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(studentXML.getBytes()));
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
            context.write(new Text("word"), new IntWritable(1));
                //context.write(NullWritable.get(), new IntWritable(1));
            //}
        }
    }

    private static class Reduce2 extends Reducer<Text, IntWritable, Text, LongWritable> {

       long sum = 0;
       int count = 0;
        Text temp;
       @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable i : values) {
                sum += i.get();
                count += 1;
            }
            temp = key;
            //context.write(key, new LongWritable(sum));
            //context.write(NullWritable.get(), new LongWritable(sum));
        }

       @Override
       protected void cleanup(Context context) throws IOException, InterruptedException {
           context.write(temp, new LongWritable(count));
       }

   }
}
