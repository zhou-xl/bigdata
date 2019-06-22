package com.bigdata.friends.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMap extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();

        String[] strings = line.split(":");

        String[] friendes = strings[1].split(",");

        v.set(strings[0]);

        for (String str : friendes) {
            k.set(str);
            context.write(k, v);
        }
    }
}
