package com.guagua.outformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 21:32
 * @describe
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 防止有重复的，有几个输出几个
        String line = key.toString();
        line = line + "\r\n";
        k.set(line);
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
