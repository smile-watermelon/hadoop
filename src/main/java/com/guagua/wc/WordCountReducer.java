package com.guagua.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 13:26
 * @describe
 */

/**
 *
 * @param <KEYIN> map输出的k输入行字节偏移量
 * @param <VALUEIN> 输入类型
 * @param <KEYOUT> 输出的key的类型
 * @param <VALUEOUT> 输出的value的类型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum+= value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
