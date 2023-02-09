package com.guagua.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 15:16
 * @describe
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long flowUp;
    private long flowDown;
    private long flowSum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(flowUp);
        out.writeLong(flowDown);
        out.writeLong(flowSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        flowUp = in.readLong();
        flowDown = in.readLong();
        flowSum = in.readLong();
    }

    @Override
    public int compareTo(FlowBean bean) {
        int result;
        if (flowSum > bean.getFlowSum()) {
            result = -1;
        } else if (flowSum < bean.getFlowSum()) {
            result = 1;
        } else {
            result = 0;
        }
        return result;
    }

    public long getFlowUp() {
        return flowUp;
    }

    public void setFlowUp(long flowUp) {
        this.flowUp = flowUp;
    }

    public long getFlowDown() {
        return flowDown;
    }

    public void setFlowDown(long flowDown) {
        this.flowDown = flowDown;
    }

    public long getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(long flowSum) {
        this.flowSum = flowSum;
    }

    @Override
    public String toString() {
        return flowUp + "," + flowDown + "," + flowSum;
    }
}
