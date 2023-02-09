package com.guagua.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 15:21
 * @describe
 */
public class FlowBeanSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] counts = line.split(",");

        String phone = counts[0];
        String flowUp = counts[1];
        String flowDown = counts[2];
        String flowSum = counts[3];

        k.setFlowUp(Long.parseLong(flowUp));
        k.setFlowDown(Long.parseLong(flowDown));
        k.setFlowSum(Long.parseLong(flowSum));

        v.set(phone);

        context.write(k, v);
    }
}
