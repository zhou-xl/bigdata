package com.bigdata.join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapJoinMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {"C:\\Users\\zhouxl\\Downloads\\temp\\in", "C:\\Users\\zhouxl\\Downloads\\temp\\out"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MapJoinMain.class);

        job.setMapperClass(ReduceJoinMap.class);
        job.setReducerClass(ReduceJoinReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce端输出压缩开启
//        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        boolean result = job.waitForCompletion(true);

        System.exit(result?1:0);

    }
}
