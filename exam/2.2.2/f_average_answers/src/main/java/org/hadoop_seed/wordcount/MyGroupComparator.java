package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MyGroupComparator extends WritableComparator {
    protected MyGroupComparator() {
        super(ComKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ComKey key1 = (ComKey) w1;
        ComKey key2 = (ComKey) w2;
        if (key1.getreputation() < key2.getreputation()) {
            return -1;
        }
        else {
            if (key1.getreputation() == key2.getreputation()) {
                return 0;
            }
        }
        return 1;
    }
}