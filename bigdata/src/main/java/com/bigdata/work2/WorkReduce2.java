package com.bigdata.work2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorkReduce2 extends Reducer<Text, GradeBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<GradeBean> values, Context context) throws IOException, InterruptedException {

        String a = "";
        String b = "";
        String c = "";
        for (GradeBean item : values) {
            if (item.getScore() >= 90) {
                a += item.getName() + ",";
            } else if (item.getScore() < 80) {
                b += item.getName() + ",";
            } else {
                c += item.getName() + ",";
            }
        }

        String result = key.toString() + "\t甲级：" + a.substring(0, a.length() - 1) + "\t总人数：" + (a.length() - a.replaceAll(",", "").length()) + "\n"
                + key.toString() + "\t乙级：" + b.substring(0, b.length() - 1) + "\t总人数：" + (b.length() - b.replaceAll(",", "").length()) + "\n"
                + key.toString() + "\t丙级：" + c.substring(0, c.length() - 1) + "\t总人数：" + (c.length() - c.replaceAll(",", "").length());
        context.write(new Text(result), NullWritable.get());
    }
}
