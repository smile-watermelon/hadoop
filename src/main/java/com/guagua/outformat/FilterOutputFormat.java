package com.guagua.outformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 21:34
 * @describe
 */
public class FilterOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        String output1 = "data/filter1/filter-atguigu-output";
        String output2 = "data/filter2/filter-other-output";
        return new MyFilterRecordWriter(job, output1, output2);
    }
}
