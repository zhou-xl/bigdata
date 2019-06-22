package com.bigdata.friends.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendMap2 extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strings = value.toString().split("\t");

        v.set(strings[0]);

        String[] strings1 = strings[1].split(",");

        Arrays.sort(strings1);
        for (int i = 0; i < strings1.length; i++) {
            for (int j = 1 + i; j < strings1.length - 1; j++) {
                k.set(strings1[i] + "-" + strings1[j]);
                context.write(k, v);
            }
        }
    }
}
