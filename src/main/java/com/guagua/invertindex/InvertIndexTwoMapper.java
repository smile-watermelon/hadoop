package com.guagua.invertindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 15:01
 * @describe
 */
public class InvertIndexTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
//        a.txt--atguigu	3
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);

        context.write(k, v);
    }
}
