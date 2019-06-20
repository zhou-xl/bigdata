package com.bigdata.GroupComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMap extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] strings = line.split(" ");

        orderBean.setId(Integer.parseInt(strings[0]));
        orderBean.setPrice(Double.parseDouble(strings[2]));

        context.write(orderBean, NullWritable.get());
    }
}
