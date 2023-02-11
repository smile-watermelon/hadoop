package com.guagua.topn;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化对象实现 Writable
 *
 * @author guagua
 * @date 2023/2/8 15:33
 * @describe
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long flowUp;
    private long flowDown;
    private long flowSum;

    public FlowBean() {
    }

    public FlowBean(long flowUp, long flowDown) {
        super();
        this.flowUp = flowUp;
        this.flowDown = flowDown;
        this.flowSum = flowUp + flowDown;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.flowUp);
        out.writeLong(this.flowDown);
        out.writeLong(this.flowSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flowUp = in.readLong();
        this.flowDown = in.readLong();
        this.flowSum = in.readLong();
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
        return
                flowUp + "\t" + flowDown + "\t" + flowSum;
    }

    @Override
    public int compareTo(FlowBean o) {
        if(this.flowSum > o.getFlowSum()) {
            return -1;
        } else if (this.flowSum < o.getFlowSum()) {
            return 1;
        }
        return 0;
    }
}
