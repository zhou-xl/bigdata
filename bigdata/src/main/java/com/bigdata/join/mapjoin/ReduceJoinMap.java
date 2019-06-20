package com.bigdata.join.mapjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMap extends Mapper<LongWritable, Text, Text, TableBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件路劲
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // 获取文件name
        String fileName = fileSplit.getPath().getName();
        // 获取数据
        String line = value.toString();

        TableBean tableBean = new TableBean();
        Text text = new Text();

        if ("order.txt".equals(fileName)) {
            String[] fields = line.split("\t");
            // 数据封装
            tableBean.setOrderId(fields[0]);
            tableBean.setPId(fields[1]);
            tableBean.setAmonut(Integer.parseInt(fields[2]));
            tableBean.setFlag(0);
            tableBean.setPName("");

            text.set(fields[1]);
        } else {
            String[] fields = line.split("\t");
            // 数据封装
            tableBean.setOrderId("");
            tableBean.setPId(fields[0]);
            tableBean.setAmonut(0);
            tableBean.setFlag(1);
            tableBean.setPName(fields[1]);

            text.set(fields[0]);
        }
        context.write(text, tableBean);
    }


}
