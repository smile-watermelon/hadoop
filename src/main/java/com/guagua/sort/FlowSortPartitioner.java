package com.guagua.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author guagua
 * @date 2023/2/9 16:05
 * @describe
 */
public class FlowSortPartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean bean, Text text, int numPartitions) {
        String phone = text.toString();
        String prePhoneNum = phone.substring(0, 3);
        int partition = 4;
        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }
        return partition;
    }
}
