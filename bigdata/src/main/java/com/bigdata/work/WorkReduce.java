package com.bigdata.work;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;

public class WorkReduce extends Reducer<Text, LongWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        List<String> list = new ArrayList<String>();
        for (LongWritable item : values) {
            list.add(item.toString());
        }

        LongSummaryStatistics lss = list.stream().collect(Collectors.summarizingLong(Long :: parseLong ));

        String result = key.toString() + "\t最高分：" + lss.getMax() + "\t平均成绩："
                + Math.round(lss.getAverage()) + "\t最低分：" + lss.getMin();

        context.write(new Text(result), NullWritable.get());
    }
}
