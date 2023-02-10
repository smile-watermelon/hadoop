package com.guagua.reducejoin;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/10 10:15
 * @describe
 */
//public class TableBean implements WritableComparable<TableBean> {
public class TableBean implements Writable {

    // 订单id
    private String id;
    // 产品id
    private String pid;
    // 数量
    private int amount;
    // 产品名称
    private String productName;
    // 标记，标记是订单表还是产品表
    private String flag;

    public TableBean() {
        super();
    }

    public TableBean(String id, String pid, int amount, String productName, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.productName = productName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(productName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        productName = in.readUTF();
        flag = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "," + productName + "," + amount;
    }

//    /**
//     * @param bean the object to be compared.
//     * @return
//     */
//    @Override
//    public int compareTo(TableBean bean) {
//        int id = Integer.parseInt(this.id);
//        int beanId = Integer.parseInt(bean.getId());
//        if (id > beanId) {
//            return -1;
//        } else if (id < beanId) {
//            return 1;
//        }
//        return 0;
//    }
}
