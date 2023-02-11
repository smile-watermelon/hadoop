package com.guagua.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 17:57
 * @describe
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private long orderId;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(long orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean orderBean) {
        if (orderId > orderBean.getOrderId()) {
            return 1;
        } else if (orderId < orderBean.getOrderId()) {
            return -1;
        } else {
            if (price > orderBean.getPrice()) {
                return -1;
            } else if (price < orderBean.getPrice()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readLong();
        price = in.readDouble();
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "," + price;
    }
}
