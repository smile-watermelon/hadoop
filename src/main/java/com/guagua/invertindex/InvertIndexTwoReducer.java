package com.guagua.invertindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 14:54
 * @describe
 */
public class InvertIndexTwoReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString().replace("\t", "-->")).append("\t");
        }
        v.set(builder.toString());
        context.write(key, v);
    }
}
