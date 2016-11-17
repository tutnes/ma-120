package org.hadoop_seed.wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by tarje on 17.11.2016.
 */
public class MyKeySortComparator extends WritableComparator {

    protected MyKeySortComparator() {
        super(ComKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ComKey key1 = (ComKey) w1;
        ComKey key2 = (ComKey) w2;

     /*   int cmpResult = key1.getuserName().compareTo(key2.getuserName());
        if (cmpResult == 0)// same deptNo
        {
            return -key1.getuserName()
                    .compareTo(key2.getuserName());
            //If the minus is taken out, the values will be in
            //ascending order
        }
        return cmpResult;*/

        //}
        if (key1.getreputation() < key2.getreputation()) {
            return 1;
        } else {
            if (key1.getreputation() == key2.getreputation()) {
                return 0;
            }
        }
        return -1;
    }
}
