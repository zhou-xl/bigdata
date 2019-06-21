package com.bigdata.work;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;

public class WorkReduce extends Reducer<Text, IntWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        List<Integer> list = new ArrayList<Integer>();
        for (IntWritable item : values) {
            list.add(item.get());
        }

        LongSummaryStatistics lss = list.stream().collect(Collectors.summarizingLong(Integer :: intValue ));

        String result = key.toString() + "\t最高分：" + lss.getMax() + "\t平均成绩："
                + Math.round(lss.getAverage()) + "\t最低分：" + lss.getMin();

        context.write(new Text(result), NullWritable.get());
    }
}
