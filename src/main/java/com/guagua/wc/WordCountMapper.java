package com.guagua.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 13:26
 * @describe
 */

/**
 *
 * @param <KEYIN> 输入行字节偏移量
 * @param <VALUEIN> 输入类型
 * @param <KEYOUT> 输出的key的类型
 * @param <VALUEOUT> 输出的value的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();

    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);

            context.write(k, v);
        }
    }
}
