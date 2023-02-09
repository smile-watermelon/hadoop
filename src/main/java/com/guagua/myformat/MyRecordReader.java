package com.guagua.myformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 22:09
 * @describe
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;

    Configuration conf;

    Text key = new Text();

    BytesWritable v = new BytesWritable();

    boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
//        1、获取fs 对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
//        2、获取输入流
            FSDataInputStream fis = fs.open(path);

            // 下面注释部分适用于hadoop 2.7.2版本
            byte[] buf = new byte[(int) split.getLength()];
            IOUtils.readFully(fis, buf, 0, buf.length);
            v.set(buf, 0, buf.length);

            key.set(path.toString());

            // 关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;

            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
