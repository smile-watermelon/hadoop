package com.guagua.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author guagua
 * @date 2023/2/10 11:19
 * @describe
 */
public class TableGroupingComparator extends WritableComparator {

    public TableGroupingComparator() {
//        super(TableBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TableBean bean1 = (TableBean) a;
        TableBean bean2 = (TableBean) b;
        int id1 = Integer.parseInt(bean1.getId());
        int id2 = Integer.parseInt(bean2.getId());
        if (id1 > id2) {
            return 1;
        } else if (id1 < id2) {
            return -1;
        } else {
            return 0;
        }
    }
}
