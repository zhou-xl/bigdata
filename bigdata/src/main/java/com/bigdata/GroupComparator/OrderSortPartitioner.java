package com.bigdata.GroupComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderSortPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getId() & Integer.MAX_VALUE) % i;
    }
}
