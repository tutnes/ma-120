package org.hadoop_seed.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


//Heavily inspired by :https://gist.github.com/airawat/6604175#file-02a-dataandcodedownload


public class ComKey implements Writable,
        WritableComparable<ComKey> {

    private String userName;
    private int reputation;

    public ComKey() {
    }

    public ComKey(String userName, int reputation) {
        this.userName = userName;
        this.reputation = reputation;
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(userName).append("\t")
                .append(reputation)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        userName = WritableUtils.readString(dataInput);
        reputation = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, userName);
        WritableUtils.writeVInt(dataOutput, reputation);
    }

    public int compareTo(ComKey objKeyPair) {
        if (reputation < objKeyPair.reputation) {
            return -1;
        }
        else {
            if (reputation == objKeyPair.reputation) {
                return 0;
            }
        return 1;
        }
    }

    public String getuserName() {
        return userName;
    }

    public void setuserName(String userName) {
        this.userName = userName;
    }

    public int getreputation() {
        return reputation;
    }

    public void setreputation(int reputation) {
        this.reputation = reputation;
    }
}