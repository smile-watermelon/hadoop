package com.guagua.myformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 22:31
 * @describe
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {


    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        context.write(key, value);

    }
}
