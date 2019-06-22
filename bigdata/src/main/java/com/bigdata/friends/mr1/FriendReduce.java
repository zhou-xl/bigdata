package com.bigdata.friends.mr1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text item : values) {
            sb.append(item).append(",");
        }

        context.write(key, new Text(sb.toString().substring(0, sb.lastIndexOf("."))));
    }
}
