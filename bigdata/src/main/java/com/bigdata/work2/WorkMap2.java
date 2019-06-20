package com.bigdata.work2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorkMap2 extends Mapper<LongWritable, Text, Text, GradeBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] strings = line.split(",");

        GradeBean gradeBean = new GradeBean();
        gradeBean.setName(strings[1]);
        gradeBean.setScore(Integer.parseInt(strings[2]));

        context.write(new Text(strings[0]), gradeBean);
    }
}
