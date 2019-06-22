package com.bigdata.friends;

import com.bigdata.common.CommonUtil;
import com.bigdata.friends.mr1.FriendMap;
import com.bigdata.friends.mr1.FriendReduce;
import com.bigdata.friends.mr2.FriendMap2;
import com.bigdata.friends.mr2.FriendReduce2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"C:\\Users\\zhouxl\\Downloads\\temp\\friends.txt", "C:\\Users\\zhouxl\\Downloads\\temp\\out", "C:\\Users\\zhouxl\\Downloads\\temp\\out2"};

        // 删除输出文件夹
        CommonUtil.deleteFile(args[1], args[2]);

        // 获取配置文件
        Configuration configuration = new Configuration();

        // 创建job任务
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FriendMain.class);

        // 指定Map类和map的输出类型
        job.setMapperClass(FriendMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定Reducer和reduce的输出类型
        job.setReducerClass(FriendReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定数据输入的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 创建job2任务
        Job job2 = Job.getInstance(configuration);
        job2.setJarByClass(FriendMain.class);

        // 指定Map类和map的输出类型
        job2.setMapperClass(FriendMap2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // 指定Reducer和reduce的输出类型
        job2.setReducerClass(FriendReduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 指定数据输入的路径
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // 分别创建两个controlledJob对象，处理两个mapReduce程序
        ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        // 创建一个管理组control，用于管理创建的controlledJob对象，自定义组名
        JobControl jobControl = new JobControl("friend");

        // 两个任务的关联方式
        controlledJob2.addDependingJob(controlledJob);

        // addJob方法添加进组
        jobControl.addJob(controlledJob);
        jobControl.addJob(controlledJob2);

        // 设置线程对象来启动job。
        Thread thread = new Thread(jobControl);
        thread.start();

        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }

        System.exit(0);
    }
}
