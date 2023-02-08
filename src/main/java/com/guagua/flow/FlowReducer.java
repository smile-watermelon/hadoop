package com.guagua.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 15:38
 * @describe
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long up  = 0;
        long down = 0;
        for (FlowBean value : values) {
            up += value.getFlowUp();
            down += value.getFlowDown();
            sum += up + down;
        }

        v.setFlowUp(up);
        v.setFlowDown(down);
        v.setFlowSum(sum);

        context.write(key, v);
    }
}
