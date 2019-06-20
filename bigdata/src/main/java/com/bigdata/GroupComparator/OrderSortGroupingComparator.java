package com.bigdata.GroupComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {

    protected OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;

        if (abean.getId() > bbean.getId()) {
            return 1;
        } else if (abean.getId() < bbean.getId()) {
            return -1;
        } else {
            return 0;
        }
    }
}
