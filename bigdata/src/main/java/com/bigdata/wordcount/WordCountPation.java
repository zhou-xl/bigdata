package com.bigdata.wordcount;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;
import java.util.Arrays;

public class WordCountPation extends Partitioner<Text, Text> {

    public int getPartition(Text text, Text text2, int i) {

        String[] YD = new String[] {"134","135","136","137","138","139","150","151","152","157","158","159","188","187","182","183","184","178","147","172","198"};

        String[] DX = new String[] {"133","149","153","173","177","180","181","189","199"};

        String[] LT = new String[] {"130","131","132","145","155","156","166","171","175","176","185","186","166"};

        String str = text2.getValue();

        String[] split = str.split("");
//        if (Arrays.asList(YD).contains())


        return 0;
    }
}
