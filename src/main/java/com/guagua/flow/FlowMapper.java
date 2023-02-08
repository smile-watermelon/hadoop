package com.guagua.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 15:38
 * @describe
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        System.out.println(fields.length);
        String phone = fields[1];
        long flowUp = Long.parseLong(fields[fields.length - 3]);
        long flowDown = Long.parseLong(fields[fields.length - 2]);

        k.set(phone);

        v.setFlowUp(flowUp);
        v.setFlowDown(flowDown);
        v.setFlowSum(flowUp + flowDown);

        context.write(k, v);
    }
}
