package com.bigdata.GroupComparator;

import com.bigdata.wordcount.WordCountMap;
import com.bigdata.wordcount.WordCountReduce;
import com.bigdata.work.WorkMap;
import com.bigdata.work.WorkReduce;
import com.bigdata.work2.GradeBean;
import com.bigdata.work2.WorkMap2;
import com.bigdata.work2.WorkReduce2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class OrderMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"C:\\Users\\zhouxl\\Downloads\\temp\\work1.txt", "C:\\Users\\zhouxl\\Downloads\\temp\\out"};
        // 获取配置文件
        Configuration configuration = new Configuration();

        // 创建job人任务
        Job job = Job.getInstance(configuration);
        job.setJarByClass(OrderMain.class);

        // 指定Map类和map的输出类型
        job.setMapperClass(WorkMap2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GradeBean.class);

        // 指定Reducer和reduce的输出类型
        job.setReducerClass(WorkReduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定数据输入的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);
//
//        job.setPartitionerClass(OrderSortPartitioner.class);
//
//        job.setNumReduceTasks(3);

        // 提交任务
        job.waitForCompletion(true);
    }
}
