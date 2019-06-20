package com.bigdata.join.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapJoin extends Mapper<LongWritable, Text, Text, NullWritable> {


    Map map = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取缓存的文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\zhouxl\\Downloads\\temp\\product.txt"),"utf-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切分
            String[] strings = line.split("\t");
            map.put(strings[0], strings[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取大表（订单）
        String string = value.toString();
        // 切分
        String[] strings = string.split("\t");

        String pid = strings[1];
        if (map.containsKey(pid)) {

            context.write(new Text(strings[0] + "\t" +map.get(pid) + "\t" + strings[2]), NullWritable.get());
        }

    }
}
