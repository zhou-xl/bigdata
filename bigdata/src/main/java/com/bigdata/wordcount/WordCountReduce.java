package com.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 初始化计数器
        int count = 0;

        // 开始计数
        for (IntWritable value : values) {
            count += value.get();
        }

        // 输出
        context.write(key, new IntWritable(count));
    }
}
