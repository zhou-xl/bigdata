package com.bigdata.join.mapjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 创建list，存储订单表的对象
        ArrayList<TableBean> orderbeans = new ArrayList<TableBean>();

        // 存储产品表
        TableBean pbbean = new TableBean();

        for (TableBean item : values) {
            // 0：订单表
            if (item.getFlag() == 0) {
                TableBean bean = new TableBean();

                try {
                    BeanUtils.copyProperties(bean, item);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderbeans.add(bean);
            } else {
                try {
                    BeanUtils.copyProperties(pbbean, item);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }
        for (TableBean item2 : orderbeans) {

            item2.setPName(pbbean.getPName());

            context.write(item2, NullWritable.get());
        }

    }
}
