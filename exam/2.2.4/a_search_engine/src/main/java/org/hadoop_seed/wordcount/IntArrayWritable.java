package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }


    /*public String toString() {
        IntWritable[] values = get();
        return values[0].toString() + ", " + values[1].toString();
    }*/
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");

        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }

        sb.append("]");
        return sb.toString();
    }

}