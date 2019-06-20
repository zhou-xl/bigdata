package com.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 拿到数据
        String line = value.toString();

        // 按照空格切分
        String[] split = line.split(" ");

        // 输出数据
        for (String str : split) {
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
