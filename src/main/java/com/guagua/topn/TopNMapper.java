package com.guagua.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author guagua
 * @date 2023/2/8 15:38
 * @describe
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

//    Text v = new Text();

    TreeMap<FlowBean, Text> flowMap = new TreeMap<>();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[0];
        long flowUp = Long.parseLong(fields[1]);
        long flowDown = Long.parseLong(fields[2]);
        long flowSum = Long.parseLong(fields[3]);

        Text v = new Text();
        v.set(phone);

        FlowBean k = new FlowBean();
        k.setFlowUp(flowUp);
        k.setFlowDown(flowDown);
        k.setFlowSum(flowSum);

        flowMap.put(k, v);
        if (flowMap.size() > 10) {
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean k = iterator.next();
            context.write(k, flowMap.get(k));
        }
    }
}
