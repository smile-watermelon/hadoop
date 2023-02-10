package com.guagua.outdb;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 22:24
 * @describe
 */
public class LogDBMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k.set(value.toString());
        context.write(k, NullWritable.get());
    }
}
