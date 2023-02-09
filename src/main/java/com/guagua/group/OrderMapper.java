package com.guagua.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 18:05
 * @describe
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean k = new OrderBean();

    /**
     * 输入数据 0000002	Pdt_05	722.4
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String orderId = fields[0];
        String price = fields[2];

        k.setOrderId(Long.parseLong(orderId));
        k.setPrice(Double.parseDouble(price));

        context.write(k, NullWritable.get());
    }





}
