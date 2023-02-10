package com.guagua.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/10 10:23
 * @describe
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean tableBean = new TableBean();

    Text k = new Text();
    
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (name.startsWith("order")) { // 订单表
            String[] fields = line.split("\t");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setProductName("");
            tableBean.setFlag("order");

            k.set(fields[1]);

        } else {    // 产品表
            String[] fields = line.split("\t");
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setProductName(fields[1]);
            tableBean.setFlag("pd");

            k.set(fields[0]);
        }
        context.write(k, tableBean);
    }
}
