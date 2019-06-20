package com.bigdata.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        args = new String[]{"C:\\temp\\wordtest.txt", "C:\\temp\\test"};
        // 获取配置文件
        Configuration configuration = new Configuration();

        // 创建job人任务
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountMain.class);

        // 指定Map类和map的输出类型
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定Reducer和reduce的输出类型
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定数据输入的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        job.waitForCompletion(true);
    }
}
