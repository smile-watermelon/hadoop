package com.guagua.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author guagua
 * @date 2023/2/9 18:26
 * @describe
 */
public class OrderGroupingComparator extends WritableComparator {

    public OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            return 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            return -1;
        } else {
            return 0;
        }
    }
}
