package com.bigdata.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMap extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean fb = new FlowBean();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        fb.setUpflow(Long.parseLong(split[1]));
        fb.setDownflow(Long.parseLong(split[2]));
        fb.setSumflow(Long.parseLong(split[3]));

        context.write(fb, new Text(split[0]));
    }
}
