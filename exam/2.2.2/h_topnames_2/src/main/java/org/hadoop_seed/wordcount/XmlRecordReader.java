package org.hadoop_seed.wordcount;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class XmlRecordReader extends RecordReader<LongWritable, Text> {
    private byte[] tagName;
    private byte[] tagNameEnd;
    private long start;
    private long end;
    private FSDataInputStream inputStream;
    private LongWritable currentKey;
    private Text currentValue;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    public XmlRecordReader() {
        try {
            this.tagName = ("<row ").getBytes("UTF-8");
            this.tagNameEnd = (" />").getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit filesplit = (FileSplit) split;
        start = filesplit.getStart();
        end = start + filesplit.getLength();
        final FileSystem fs = filesplit.getPath().getFileSystem(context.getConfiguration());
        inputStream = fs.open(filesplit.getPath());
        inputStream.seek(start);
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        final long total = end - start;
        return (inputStream.getPos() - start) / total;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (inputStream.getPos() < end) {
            if (readUntilMatch(tagName, false)) {
                try {
                    buffer.write(tagName);
                    if (readUntilMatch(tagNameEnd, true)) {
                        currentKey = new LongWritable(inputStream.getPos());
                        currentValue = new Text();
                        currentValue.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    private boolean readUntilMatch(byte[] match, boolean appendBytes) throws IOException {
        int position = 0;
        while (true) {
            int nextByte = inputStream.read();
            if (nextByte == -1) {
                return false;
            }
            if (appendBytes) {
                buffer.write(nextByte);
            }
            if (nextByte == match[position]) {
                position++;
                if (position >= match.length) {
                    return true;
                }
            } else {
                position = 0;
            }
            if (!appendBytes && position == 0 && inputStream.getPos() >= end) {
                return false;
            }
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}