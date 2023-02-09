package com.guagua.wc;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 16:36
 * @describe
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum  += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
