package com.guagua.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author guagua
 * @date 2023/2/8 15:38
 * @describe
 */
public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    TreeMap<FlowBean, Text> flowMap = new TreeMap<>();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean flowBean = new FlowBean();
            flowBean.setFlowUp(key.getFlowUp());
            flowBean.setFlowDown(key.getFlowDown());
            flowBean.setFlowSum(key.getFlowSum());

            // 这里需要特别注意，value 是内存中一个对象的引用，不是多个对象的引用
            flowMap.put(flowBean, new Text(value));

            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean k = iterator.next();
            context.write(flowMap.get(k), k);
        }
    }
}
